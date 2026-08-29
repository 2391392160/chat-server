# ChatServer

基于 Reactor 模式的高并发聊天服务器（C++11/Linux）

## 技术栈

- C++11
- epoll ET + 非阻塞 IO
- 自定义线程池
- 自定义二进制协议（4字节长度头 + 1字节类型 + payload）
- spdlog 日志
- CMake 构建

## 架构设计

- **Reactor 模式**：主线程 epoll_wait 监听事件，将 fd 派发到线程池处理
- **LT + ET 混合**：listen_fd 用 LT，client_fd 用 ET
- **自定义协议**：4 字节长度头 + 1 字节类型 + payload，解决粘包/半包
- **标记删除**：g_closing_fds 解决 fd 竞态，避免 EBADF
- **锁粒度控制**：群发时锁内只拷贝 fd 列表，锁外执行 send

## 压测数据

| 并发连接 | 消息数 | 丢包率 | QPS | 平均延迟 |
|---|---|---|---|---|
| 500    | 5000   | 0%    | 8196 | 7354us    |

> 压测方式：500 个客户端同时连接，登录完成后统一开始发送，每个客户端发送 10 条消息。QPS 测量纯消息收发性能，不含连接建立和登录时间。

## 快速开始

```bash
mkdir build && cd build
cmake .. -DCMAKE_CXX_FLAGS="-O0"
make -j2
./server
```
