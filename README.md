# TCP Chat Server

基于 epoll ET + 线程池的高并发 TCP 聊天服务器。

## 技术栈

- C++11
- Linux (epoll, socket, pthread)
- 自定义二进制协议

## 架构

- **主线程**：epoll_wait + accept，LT 模式监听端口
- **工作线程**：4 线程池处理读写业务，ET 模式触发
- **协议层**：自定义二进制协议，4 字节长度头 + 1 字节类型 + payload

## 编译

```bash
mkdir build && cd build
cmake ..
make -j
