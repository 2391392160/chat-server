#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import socket
import struct
import time

def send_msg(sock, msg_type, data):
    """
    发送二进制协议包
    格式: 4字节长度头 + 1字节类型 + 变长数据
    """
    data_bytes = data.encode('utf-8')
    total_len = 4 + 1 + len(data_bytes)
    # 打包: >I 表示4字节无符号整数（网络字节序，大端）
    packet = struct.pack('>I', total_len) + bytes([msg_type]) + data_bytes
    sock.send(packet)

def main():
    # 连接到服务端
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('127.0.0.1', 8888))
    print("✅ 已连接到 127.0.0.1:8888")

    # 1. 发送登录包
    #    MSG_LOGIN = 0x01, 用户名 = "张三"
    send_msg(sock, 0x01, "张三")
    print("📤 发送登录包: 用户名=张三")
    time.sleep(0.5)  # 等0.5秒，让服务端有时间处理

    # 2. 发送群聊包
    #    MSG_BROADCAST = 0x02, 内容 = "大家好，我是张三！"
    send_msg(sock, 0x02, "大家好，我是张三！")
    print("📤 发送群聊包: 大家好，我是张三！")
    time.sleep(0.5)

    # 3. 发送心跳包
    #    MSG_HEARTBEAT = 0x04, 内容为空
    send_msg(sock, 0x04, "")
    print("❤️ 发送心跳包")
    time.sleep(0.5)

    sock.close()
    print("🔌 断开连接")

if __name__ == "__main__":
    main()
