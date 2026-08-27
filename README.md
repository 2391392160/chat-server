# ChatServer

基于 Reactor 模式的高并发聊天服务器（C++11/Linux）

## 技术栈
- C++11
- epoll ET + 非阻塞 IO
- 自定义线程池
- 自定义二进制协议（4字节长度头 + 1字节类型 + payload）
- spdlog 日志
- CMake 构建

## 压测数据
| 并发连接 | 消息数 | 丢包率 | QPS | 平均延迟 |
|---|---|---|---|---|
| 500    | 5000   | 0%    | 4149 | 424us    |

## 快速开始
```bash
mkdir build && cd build
cmake .. -DCMAKE_CXX_FLAGS="-O0"
make -j2
./server
```
