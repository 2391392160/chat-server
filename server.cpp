
#include <iostream>
#include <cstring>
#include <functional>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/wait.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <csignal>
#include "thread_pool.h"
#include <mutex>
#include "protocol.h"
#include <unordered_map>
#include <unordered_set>
#include "spdlog/spdlog.h"
#define HEARTBEAT_TIMEOUT 60  // 秒
#define EPOLL_TIMEOUT_MS 5000 // 毫秒

int g_epfd = -1; // 全局 epoll 文件描述符，供线程池使用 (监控)

std::unordered_map<int, std::string> g_recv_bufs; // 每个 fd 的累积缓冲区
std::mutex g_buf_mutex;                           // 保护 g_recv_bufs
// fd → 用户信息
struct UserInfo
{
   std::string username;
   time_t last_heartbeat; // 最后收到心跳的时间（秒级时间戳）
};
std::unordered_map<int, UserInfo> g_users;
std::mutex g_users_mutex;
// "待关闭集合"  解决 ->心跳线程和工作线程在抢同一个 fd
std::unordered_set<int> g_closing_fds;
std::mutex g_closing_mutex;
// ========== 设置文件描述符为非阻塞 ==========
// 参数 fd: 要设置的套接字
// 返回: 成功 0，失败 -1
// 原理: 用 fcntl 获取当前 flags，再按位或上 O_NONBLOCK
// 为什么必须: ET 模式下必须配合非阻塞 IO，否则 recv 读空时会永远阻塞
int set_nonblocking(int fd)
{
   int flags = fcntl(fd, F_GETFL, 0);
   if (flags == -1)
      return -1;
   return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}
// 返回值：true 成功，false 表示对端已死，需要 close(fd)
bool safe_send(int fd, const std::string &packet)
{
   size_t total_sent = 0; // 已经发了多少字节

   while (total_sent < packet.size())
   {
      // 发剩余的数据
      ssize_t n = send(fd, packet.data() + total_sent,
                       packet.size() - total_sent, MSG_NOSIGNAL);

      if (n > 0)
      {
         // 正常发了 n 字节
         total_sent += n;
      }
      else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
      {
         // 内核缓冲区满了，睡 1 毫秒再试
         usleep(1000);
         continue;
      }
      else
      {
         // 真正的错误：对端断开、网络错误等
         return false; // 告诉调用者：这个 fd 死了，需要 close
      }
   }
   return true; // 全部发完了
}
// 心跳超时检测函数
void check_heartbeat_timeout()
{
   time_t now = time(nullptr);   // 获取当前时间戳（秒）
   std::vector<int> timeout_fds; // 存放超时fd的临时容器
   // ==========锁内只读，收集超时 fd ==========
   {
      std::lock_guard<std::mutex> lock(g_users_mutex);
      for (const auto &pair : g_users)
      {
         if (now - pair.second.last_heartbeat > HEARTBEAT_TIMEOUT)
         {
            timeout_fds.push_back(pair.first);
         }
      }
   } //  锁在这里释放，RAII 机制。构造时加锁，析构时自动解锁

   // ========== 锁外逐个踢人，发送通知并清理资源 ==========
   for (int fd : timeout_fds)
   {
      spdlog::warn("[心跳超时] fd={}将被踢出", fd);
      // 标记该 fd 即将被关闭，防止工作线程继续操作
      {
         std::lock_guard<std::mutex> lock(g_closing_mutex);
         g_closing_fds.insert(fd);
      }
      // 通知客户端（告知被踢原因），发不出去也没关系，反正都要踢
      std::string kick_pkt = encode_packet(MSG_KICK, "heartbeat timeout");
      bool ok = safe_send(fd, kick_pkt); // safe_send 内部处理 EAGAIN
      if (!ok)
      {
         spdlog::error("[心跳超时] fd={} 通知失败，客户端可能已死", fd);
      }

      //  清理资源
      //  【新增】从 epoll 删除
      if (g_epfd >= 0)
      {
         epoll_ctl(g_epfd, EPOLL_CTL_DEL, fd, nullptr);
      }

      //  【新增】清理接收缓冲区
      {
         std::lock_guard<std::mutex> lock(g_buf_mutex);
         g_recv_bufs.erase(fd);
      }

      // 【新增】清理用户信息
      {
         std::lock_guard<std::mutex> lock(g_users_mutex);
         auto it = g_users.find(fd);
         if (it != g_users.end())
         {
            spdlog::warn("[下线] 用户 {} (fd={}) 心跳超时", it->second.username, fd);
            g_users.erase(it);
         }
      }

      // 关闭 fd
      close(fd);

      //  从待关闭集合移除
      {
         std::lock_guard<std::mutex> lock(g_closing_mutex);
         g_closing_fds.erase(fd);
      }
   }
}
void handle_client(int fd)
{
   bool need_close = false; // 统一关闭标志 解决 send 失败 break 后的泄漏
   // ET 模式下必须 while 循环读，直到读空（返回 EAGAIN）
   // 检查该 fd 是否已被心跳线程标记为即将关闭

   {
      std::lock_guard<std::mutex> lock(g_closing_mutex);
      if (g_closing_fds.count(fd))
      {
         need_close = true; // ✅ 标记需要关闭
      }
   }
   if (need_close)
   {
      goto cleanup; // 跳到函数末尾的统一清理代码
   }
   char tmp_buf[1024]; // 临时接收缓冲区
   ssize_t n;

   // fd 已设为非阻塞，recv 没数据时返回 -1, errno=EAGAIN，不会挂起
   while ((n = recv(fd, tmp_buf, sizeof(tmp_buf), 0)) > 0)
   {
      //  追加到全局缓冲区  ：一次 recv 可能收到多个完整包，所以用 while 反复拆
      {
         std::lock_guard<std::mutex> lock(g_buf_mutex);
         g_recv_bufs[fd].append(tmp_buf, n);
         // ====== 上限检查 ======
         const size_t MAX_RECV_BUF = 10 * 1024 * 1024; // 10MB
         if (g_recv_bufs[fd].size() > MAX_RECV_BUF)
         {
            spdlog::warn("[安全] fd={} 接收缓冲区超限 ({:.2f} MB)，强制关闭", fd, g_recv_bufs[fd].size() / 1024.0 / 1024.0);
            need_close = true;
         }
      }
      if (need_close)
         break;
      // 2. 循环拆包
      while (true)
      {
         uint8_t type;
         std::string payload; // 用于存储解析出来的消息体（变长数据）
         bool has_packet;

         {
            // 拆包，业务处理在锁外执行
            std::lock_guard<std::mutex> lock(g_buf_mutex);
            has_packet = decode_packet(g_recv_bufs[fd], type, payload);
         }
         // 拆不出来 → 数据不够一个完整包，退出循环等下一次 recv
         if (!has_packet)
            break;

         //  处理业务
         switch (type)
         {
         case MSG_LOGIN:
         {
            // 登记用户
            size_t count;
            {
               std::lock_guard<std::mutex> lock(g_users_mutex);
               UserInfo info; // 包含用户名和心跳时间
               info.username = payload;
               info.last_heartbeat = time(nullptr); // 记录登录时间
               g_users[fd] = info;
               count = g_users.size();
            }
            spdlog::info("[登录] 用户 {} (fd={}) 上线，当前在线 {} 人", payload, fd, count);

            // 回复欢迎消息（客户端会显示 [系统] Welcome, xxx!）
            std::string reply = "Welcome, " + payload + "!";
            std::string packet = encode_packet(MSG_SYSTEM, reply);
            if (!safe_send(fd, packet))
            {
               need_close = true;
            }
            break;
         }

         case MSG_BROADCAST:
         {
            // ===== 锁粒度控制 =====
            std::string sender_name;
            std::vector<int> online_fds;

            // 步骤1：锁内只读数据、拷贝 fd 列表，不做 IO
            {
               std::lock_guard<std::mutex> lock(g_users_mutex);
               auto it = g_users.find(fd);
               if (it != g_users.end())
               {
                  sender_name = it->second.username;
               }
               else
               {
                  sender_name = "unknown";
               }
               for (const auto &pair : g_users)
               {
                  online_fds.push_back(pair.first);
               }
            }

            // 步骤2：锁外构造消息、发送
            std::string full_msg = sender_name + ": " + payload;
            spdlog::info("[群聊] {}", full_msg);
            std::string packet = encode_packet(MSG_BROADCAST, full_msg);

            for (int target_fd : online_fds)
            {
               if (target_fd == fd)
                  continue; // 跳过自己
               if (!safe_send(target_fd, packet))
               {
                  // ✅ 只标记，不 close（让持有该 fd 的工作线程自己清理）
                  {
                     std::lock_guard<std::mutex> lock(g_closing_mutex);
                     g_closing_fds.insert(target_fd);
                  }
               }
            }
            break;
         }

         case MSG_HEARTBEAT:
         {
            // 更新心跳时间
            {
               std::lock_guard<std::mutex> lock(g_users_mutex);
               auto it = g_users.find(fd);
               if (it != g_users.end())
               {
                  it->second.last_heartbeat = time(nullptr);
               }
            }
            // 回 PONG
            std::string packet = encode_packet(MSG_HEARTBEAT, "");
            if (!safe_send(fd, packet))
            {
               need_close = true;
            }
            break;
         }
         default:
            spdlog::warn("未知类型: {}", type);
            break;
         }
         if (need_close)
            break;
         // ====== 二次检查：业务处理完后，检查是否被主线程标记 ======
         {
            std::lock_guard<std::mutex> lock(g_closing_mutex);
            if (g_closing_fds.count(fd))
            {
               need_close = true;
               break;
            }
         }
         // ==============================================
      }
      if (need_close)
         break;
   }

   // n == 0：对端正常关闭（客户端调了 close）
   // n < 0 && errno != EAGAIN：真正的错误（不是非阻塞读空）
   // n < 0 && errno == EAGAIN：读空了，正常返回，连接保持
   if (n == 0)
   {
      spdlog::info("客户端 fd={} 正常断开", fd);
      need_close = true;
   }
   else if (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK)
   {
      if (errno == EBADF)
      {
         // ✅ 修正：走 cleanup，不直接 return（防止 fd 重用污染）
         need_close = true;
         goto cleanup;
      }
      // 真正的错误：如 ECONNRESET（对端崩溃）、EIO 等
      spdlog::error("recv 错误: {}", strerror(errno));

      // 同样标记关闭，统一处理
      need_close = true; // 真正的错误，必须关闭
   }
cleanup:
   // 统一清理
   if (need_close)
   {
      // 先通知 epoll 移除该 fd，再 close
      if (g_epfd >= 0)
      {
         epoll_ctl(g_epfd, EPOLL_CTL_DEL, fd, nullptr);
      }

      {
         std::lock_guard<std::mutex> lock(g_buf_mutex);
         g_recv_bufs.erase(fd); //
      }
      // 删用户
      {
         std::lock_guard<std::mutex> lock(g_users_mutex);
         auto it = g_users.find(fd);
         if (it != g_users.end())
         {
            spdlog::info("[下线] 用户 {} (fd={}) 断开", it->second.username, fd);
            g_users.erase(it);
         }
      }
      // 从待关闭集合中移除
      {
         std::lock_guard<std::mutex> lock(g_closing_mutex);
         g_closing_fds.erase(fd);
      }
      // 【新增】防御性：只有 fd 还合法才 close，避免二次 close
      if (fcntl(fd, F_GETFL) != -1)
      {
         close(fd);
      }
   }
   // 如果 n < 0 && errno == EAGAIN：啥也不做，连接保持，正常返回
}
int main()
{
   spdlog::set_pattern("[%Y-%m-%d %H:%M:%S] [%l] %v");
   ThreadPool pool(4); // 创建线程池
   // 忽略 SIGPIPE，防止客户端异常断开导致服务端进程被杀
   signal(SIGPIPE, SIG_IGN);
   // 创建监听套接字
   int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
   if (listen_fd < 0)
   {
      spdlog::error("创建监听listen套接字失败");
      return 0;
   }

   // 端口复用（可选，防止上次异常退出导致端口被占）
   int opt = 1;
   setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

   // bind套接字
   struct sockaddr_in local;
   memset(&local, 0, sizeof(local));
   local.sin_family = AF_INET;
   local.sin_port = htons(8888);
   local.sin_addr.s_addr = INADDR_ANY;
   if (bind(listen_fd, (struct sockaddr *)&local, sizeof(local)) < 0)
   {
      spdlog::error("bind失败: {}", strerror(errno));
      close(listen_fd);
      exit(1);
   }
   // 监听
   if (listen(listen_fd, 128) < 0)
   {
      spdlog::error("listen失败: {}", strerror(errno));
      close(listen_fd);
      exit(1);
   }
   // listen_fd 设非阻塞
   set_nonblocking(listen_fd);

   int epfd = epoll_create1(0); // 创建 epoll 实例   在内核里创建一个"红黑树 + 就绪链表"的数据结构
   g_epfd = epfd;
   if (epfd < 0)
   {

      spdlog::error("epoll_createl失败 {}", strerror(errno));
      close(listen_fd);
      return 1;
   }

   struct epoll_event ev;      // 声明一个事件结构体（临时变量）
   memset(&ev, 0, sizeof(ev)); //  清零，避免未初始化字段干扰

   ev.events = EPOLLIN;    // // 告诉内核：我只关心"可读"事件 LT模式
   ev.data.fd = listen_fd; //  告诉内核：这个 fd 的编号是 listen_fd

   if (epoll_ctl(epfd, EPOLL_CTL_ADD, listen_fd, &ev) < 0) // 把 listen_fd 注册进 epoll
   {
      spdlog::error("epoll_ctl ADD listen_fd 错误: {}", strerror(errno));
      close(listen_fd);
      close(epfd);
      return 1;
   }

   spdlog::info("Server started on port 8888 (Step 2: epoll LT)");
   struct epoll_event events[64]; // 接收 epoll_wait 的返回结果  输出数组，内核会把就绪的 fd 事件填到这里
   // 循环接受连接accept
   while (1)
   {
      //  5秒超时，用于定期扫描心跳
      int nfds = epoll_wait(epfd, events, 64, EPOLL_TIMEOUT_MS); // 参数: epfd, 输出数组, 数组容量, 超时
                                                                 // nfds:已就绪的文件描述符（事件）数量
      if (nfds < 0)
      {
         spdlog::error("epoll_wait错误: {}", strerror(errno));
         continue;
      }
      // 遍历所有就绪事件
      for (int i = 0; i < nfds; i++)
      {
         int fd = events[i].data.fd;
         // 判断事件来源
         if (fd == listen_fd)
         {
            // listen_fd 就绪 = 有新连接到达
            while (1)
            {
               struct sockaddr_in client;
               socklen_t len = sizeof(client);
               int client_fd = accept(listen_fd, (struct sockaddr *)&client, &len); // 创建一个全新的 socket

               if (client_fd < 0)
               {
                  if (errno == EAGAIN || errno == EWOULDBLOCK)
                     break;

                  spdlog::error("accept错误: {}", strerror(errno));
                  break;
               }
               // 打印客户端信息

               char ip[INET_ADDRSTRLEN];

               inet_ntop(AF_INET, &client.sin_addr, ip, sizeof(ip));

               spdlog::info("新连接 ip={} port={} fd={}", ip, ntohs(client.sin_port), client_fd);

               // ========== client_fd 非阻塞 ==========
               set_nonblocking(client_fd);

               memset(&ev, 0, sizeof(ev));
               // ========== client_fd 用 ET 模式 ==========
               ev.events = EPOLLIN | EPOLLET; //

               // 把 client_fd 也注册进 epoll  以后这个 client 发数据，epoll_wait 会通知我
               ev.data.fd = client_fd;
               if (epoll_ctl(epfd, EPOLL_CTL_ADD, client_fd, &ev) < 0)
               {

                  spdlog::error("epoll_ctl ADD client_fd 错误: {}", strerror(errno));
                  close(client_fd); // ← 只关这个客户端
                  continue;         // ← 继续处理下一个连接
               }
            }
         }
         else
         {
            // client_fd 就绪 = 有数据可读
            // 丢给线程池，主线程继续回 epoll_wait 等下一个事件
            pool.enqueue(fd);
         }
      }
      // 【新增】每次 epoll_wait 返回后，检查心跳超时
      // 即使 nfds==0（超时返回），也会执行这里
      check_heartbeat_timeout();
   }
   close(listen_fd);
   close(epfd);
   return 0;
}