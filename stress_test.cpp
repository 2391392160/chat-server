// stress_test.cpp
#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include "protocol.h"

using namespace std;

atomic<int> g_success{0};
atomic<int> g_sent{0};
atomic<int> g_recv{0};
atomic<long long> g_latency{0};

void stress_client(int id, const string &ip, int port, int msg_count)
{
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, ip.c_str(), &addr.sin_addr);

    if (connect(sock, (sockaddr *)&addr, sizeof(addr)) < 0) {
        cerr << "Client " << id << " connect failed\n";
        return;
    }
    g_success++;

    // 登录
    string login = encode_packet(MSG_LOGIN, "user_" + to_string(id));
    send(sock, login.data(), login.size(), MSG_NOSIGNAL);
    usleep(10000);

    // ====== 每个线程独立的累积缓冲区 ======
    string recv_buf;
    char buf[4096];

    // 收欢迎消息（循环拆包，确保登录完成）
    while (true) {
        int n = recv(sock, buf, sizeof(buf), 0);
        if (n <= 0) break;
        recv_buf.append(buf, n);
        uint8_t type;
        string payload;
        while (decode_packet(recv_buf, type, payload)) {
            if (type == MSG_SYSTEM) {
                goto logged_in;
            }
        }
    }
logged_in:

    for (int i = 0; i < msg_count; i++) {
        string msg = "msg_" + to_string(i);
        string pkt = encode_packet(MSG_BROADCAST, msg);

        auto start = chrono::high_resolution_clock::now();
        send(sock, pkt.data(), pkt.size(), MSG_NOSIGNAL);
        g_sent++;

        // ====== 循环拆包计数 ======
        while (true) {
            int n = recv(sock, buf, sizeof(buf), 0);
            if (n <= 0) break;
            recv_buf.append(buf, n);
            uint8_t type;
            string payload;
            while (decode_packet(recv_buf, type, payload)) {
                if (type == MSG_BROADCAST) {
                    g_recv++;
                    auto end = chrono::high_resolution_clock::now();
                    g_latency += chrono::duration_cast<chrono::microseconds>(end - start).count();
                    goto next_msg;
                }
            }
        }
    next_msg:
        usleep(1000);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    close(sock);
}

int main(int argc, char *argv[])
{
    if (argc != 5)
    {
        cout << "Usage: " << argv[0] << " <ip> <port> <connections> <msg_per_conn>\n";
        return 1;
    }

    string ip = argv[1];
    int port = stoi(argv[2]);
    int conn = stoi(argv[3]);
    int msg = stoi(argv[4]);

    auto t1 = chrono::high_resolution_clock::now();

    vector<thread> threads;
    for (int i = 0; i < conn; i++)
    {
        threads.emplace_back(stress_client, i, ip, port, msg);
        if (i % 100 == 0)
            usleep(10000);
    }

    for (auto &t : threads)
        t.join();

    auto t2 = chrono::high_resolution_clock::now();
    auto ms = chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();

    cout << "\n========== 压测报告 ==========\n";
    cout << "并发连接: " << conn << "\n";
    cout << "连接成功: " << g_success << "\n";
    cout << "消息发送: " << g_sent << "\n";
    cout << "消息接收: " << g_recv << "\n";
    cout << "总耗时:   " << ms << " ms\n";
    cout << "QPS:      " << (g_recv * 1000.0 / ms) << "\n";
    if (g_recv > 0)
    {
        cout << "平均延迟: " << (g_latency / g_recv) << " us\n";
    }
    cout << "==============================\n";
    return 0;
}