#include <iostream>
#include <cstring>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string>
#include "protocol.h"
#include <thread>
#include <atomic>
#include <errno.h>

std::atomic<bool> g_running{true};
void recv_thread(int sock_fd) {
    char buf[4096];
    std::string recv_buf;   // ← 累积缓冲区，处理粘包/半包
    
    while (g_running) {
        int n = recv(sock_fd, buf, sizeof(buf), 0);
        if (n <= 0) {
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                continue;   // 非阻塞，没数据，继续循环
            }
            std::cout << "\n[系统] 与服务器的连接已断开" << std::endl;
            g_running = false;   // ← 通知主线程和心跳线程退出
            break;
        }
        
        // 追加到累积缓冲区
        recv_buf.append(buf, n);
        
        // 循环拆包（可能一次收到多个完整包）
        uint8_t type;
        std::string payload;
        while (decode_packet(recv_buf, type, payload)) {
            switch (type) {
                case MSG_BROADCAST:
                    std::cout << "\n[群聊] " << payload << std::endl;
                    break;
                case MSG_SYSTEM:
                    std::cout << "\n[系统] " << payload << std::endl;
                    break;
                case MSG_HEARTBEAT:
                    // 收到 PONG，静默处理
                    break;
                case MSG_KICK:
                    std::cout << "\n[系统] 你已被踢出：" << payload << std::endl;
                    g_running = false;
                    break;
                default:
                    std::cout << "\n[未知] type=" << (int)type << std::endl;
                    break;
            }
            // 每处理完一条消息，重新打印输入提示符
            std::cout << "Enter msg> " << std::flush;
        }
    }
}
void heartbeat_thread(int sock_fd) {
    while (g_running) {
        std::string ping = encode_packet(MSG_HEARTBEAT, "");
        int ret = send(sock_fd, ping.data(), ping.size(), 0);
        if (ret < 0) {
            perror("[心跳] 发送失败");
            break;
        }
        // 睡 30 秒，但每秒检查一次 g_running
        for (int i = 0; i < 30 && g_running; ++i) {
            sleep(1);
        }
    }
}

int main()
{
    // 创建socket套接字
    int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd == -1)
    {
        std::cout << "创建socket失败" << std::endl;
        return -1;
    }
    // 准备服务端地址
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(8888);
    // inet_pton 把字符串形式的 IP 地址（"127.0.0.1"）转成二进制形式
    ::inet_pton(AF_INET, "127.0.0.1", &server_addr.sin_addr);

    // 连接服务端
    int ret = connect(sock_fd, (struct sockaddr *)&server_addr, sizeof(server_addr));
    if (ret == -1)
    {
        std::cerr << "连接服务端失败" << std::endl;
        close(sock_fd);
        return -1;
    }
    std::cout << "已连接到服务端" << std::endl;

    // ========== 新增：登录流程 ==========
    std::cout << "输入用户名: ";
    std::string username;
    std::getline(std::cin, username);
    
std::string login_pkt = encode_packet(MSG_LOGIN, username);
send(sock_fd, login_pkt.data(), login_pkt.size(), 0);
// ========== 登录结束 ==========

// ========== 新增：启动后台线程 ==========
std::thread hb(heartbeat_thread, sock_fd);
hb.detach();
std::thread rcv(recv_thread, sock_fd);
rcv.detach();
// ========== 线程启动结束 ==========


// ========== 改造：主线程只负责发消息 ==========
std::string msg;
while (g_running) {                    // 
    std::cout << "Enter msg> " << std::flush;
    if (!std::getline(std::cin, msg)) break;
    if (msg == "quit") break;
    
    // 关键：发消息前必须打包！
    std::string chat_pkt = encode_packet(MSG_BROADCAST, msg);
    send(sock_fd, chat_pkt.data(), chat_pkt.size(), 0);
}
// ========== 发消息结束 ==========
// 退出前通知其他线程
g_running = false;
close(sock_fd);
}
