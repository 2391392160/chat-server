// stress_test.cpp - 改进版压测工具
#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <errno.h>
#include "protocol.h"

using namespace std;

// 统计变量
atomic<int> g_success{0};          // 成功连接数
atomic<int> g_sent{0};             // 发送消息总数
atomic<int> g_recv{0};             // 收到消息总数
atomic<long long> g_latency{0};    // 总延迟（微秒）
atomic<int> g_ready{0};            // 已准备就绪的客户端数
atomic<bool> g_start{false};       // 开始信号

void stress_client(int id, const string &ip, int port, int msg_count)
{
    // 1. 创建 socket
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        cerr << "Client " << id << " socket failed\n";
        return;
    }

    sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, ip.c_str(), &addr.sin_addr);

    // 2. 连接
    if (connect(sock, (sockaddr *)&addr, sizeof(addr)) < 0) {
        cerr << "Client " << id << " connect failed: " << strerror(errno) << "\n";
        close(sock);
        return;
    }
    g_success++;

    // 3. 登录
    string login = encode_packet(MSG_LOGIN, "user_" + to_string(id));
    send(sock, login.data(), login.size(), MSG_NOSIGNAL);

    // 4. 收欢迎消息（确认登录成功）
    char buf[4096];
    string recv_buf;
    bool logged_in = false;
    for (int retry = 0; retry < 20; retry++) {
        int n = recv(sock, buf, sizeof(buf), 0);
        if (n <= 0) {
            usleep(10000);
            continue;
        }
        recv_buf.append(buf, n);
        uint8_t type;
        string payload;
        while (decode_packet(recv_buf, type, payload)) {
            if (type == MSG_SYSTEM) {
                logged_in = true;
                break;
            }
        }
        if (logged_in) break;
    }
    if (!logged_in) {
        cerr << "Client " << id << " login timeout\n";
        close(sock);
        return;
    }

    // 5. 等待开始信号（所有客户端都准备好后同时开始）
    g_ready++;
    while (!g_start.load()) {
        usleep(1000);
    }

    // 6. 发送消息（发一条等一条，测量端到端延迟）
    for (int i = 0; i < msg_count; i++) {
        string msg = "msg_" + to_string(i);
        string pkt = encode_packet(MSG_BROADCAST, msg);

        auto start = chrono::high_resolution_clock::now();

        // 发送消息
        int ret = send(sock, pkt.data(), pkt.size(), MSG_NOSIGNAL);
        if (ret < 0) {
            continue;
        }
        g_sent++;

        // 等待自己的广播回来（带超时）
        bool received = false;
        for (int timeout = 0; timeout < 50; timeout++) {
            int n = recv(sock, buf, sizeof(buf), 0);
            if (n <= 0) {
                usleep(1000);
                continue;
            }
            recv_buf.append(buf, n);
            uint8_t type;
            string payload;
            while (decode_packet(recv_buf, type, payload)) {
                if (type == MSG_BROADCAST) {
                    // 匹配成功
                    auto end = chrono::high_resolution_clock::now();
                    g_recv++;
                    g_latency += chrono::duration_cast<chrono::microseconds>(end - start).count();
                    received = true;
                    break;
                }
            }
            if (received) break;
        }
        // 如果超时没收到，这条消息就不计入延迟，但为了计数准确，尝试继续
        if (!received) {
            // 没有收到广播，可能服务器处理慢或网络延迟高
            // 记录消息发送成功，但没收到响应，计入丢包
        }
    }

    // 7. 等待所有消息处理完，然后关闭
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    close(sock);
}

int main(int argc, char *argv[])
{
    if (argc != 5) {
        cout << "Usage: " << argv[0] << " <ip> <port> <connections> <msg_per_conn>\n";
        cout << "Example: " << argv[0] << " 127.0.0.1 8888 500 10\n";
        return 1;
    }

    string ip = argv[1];
    int port = stoi(argv[2]);
    int conn = stoi(argv[3]);
    int msg = stoi(argv[4]);

    cout << "\n========== 压测配置 ==========\n";
    cout << "服务端: " << ip << ":" << port << "\n";
    cout << "并发连接: " << conn << "\n";
    cout << "每连接消息数: " << msg << "\n";
    cout << "总消息数: " << conn * msg << "\n";
    cout << "================================\n\n";

    // 1. 创建所有客户端线程（分批建立连接，避免 SYN flood）
    cout << "正在建立连接...\n";
    vector<thread> threads;
    for (int i = 0; i < conn; i++) {
        threads.emplace_back(stress_client, i, ip, port, msg);
        if (i % 100 == 0) {
            cout << "  " << i << "/" << conn << " connected\r" << flush;
            usleep(5000);
        }
    }

    // 2. 等待所有客户端准备就绪
    cout << "\n等待所有客户端登录完成...\n";
    while (g_ready.load() < conn) {
        cout << "  " << g_ready.load() << "/" << conn << " ready\r" << flush;
        usleep(100000);
    }
    cout << "\n所有客户端已就绪，开始压测...\n";

    // 3. 发送开始信号，同时开始计时
    auto t_start = chrono::high_resolution_clock::now();
    g_start.store(true);

    // 4. 等待所有线程结束
    for (auto &t : threads) {
        t.join();
    }
    auto t_end = chrono::high_resolution_clock::now();

    // 5. 计算统计
    auto total_ms = chrono::duration_cast<chrono::milliseconds>(t_end - t_start).count();

    cout << "\n\n========== 压测报告 ==========\n";
    cout << "并发连接: " << conn << "\n";
    cout << "连接成功: " << g_success.load() << "\n";
    cout << "消息发送: " << g_sent.load() << "\n";
    cout << "消息接收: " << g_recv.load() << "\n";
    cout << "丢包率:   " << (1.0 - (double)g_recv.load() / g_sent.load()) * 100 << "%\n";
    cout << "总耗时:   " << total_ms << " ms\n";
    cout << "QPS:      " << (g_recv.load() * 1000.0 / total_ms) << "\n";
    if (g_recv.load() > 0) {
        cout << "平均延迟: " << (g_latency.load() / g_recv.load()) << " us\n";
    }
    cout << "==============================\n";

    return 0;
}