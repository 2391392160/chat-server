// ============ protocol.h ============
#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <stdint.h> // uint8_t, uint16_t
#include <string>   // std::string
#include <vector>   // std::vector
#include <arpa/inet.h>
#include <string.h>
// ============ 消息类型枚举 ============
// 用数字代表不同的业务类型，发送和接收双方用同一个数字来识别
enum MsgType : uint8_t
{
   MSG_LOGIN = 0x01,     // 登录请求（客户端 → 服务端）
   MSG_BROADCAST = 0x02, // 群聊消息（客户端 → 服务端 → 转发给所有人）
   MSG_PRIVATE = 0x03,   // 私聊消息（客户端 → 服务端 → 转发给指定人）
   MSG_HEARTBEAT = 0x04, // 心跳包（客户端定时发，证明自己还活着）
   MSG_SYSTEM = 0x05,    // 系统通知：登录欢迎、在线人数、被踢提示
   MSG_KICK = 0x06,      // 被踢出通知
};

// ============ 打包函数 ============
inline std::string encode_packet(uint8_t type, const std::string &content)
{
   std::string packet;
   // 总长度 = 4(长度头) + 1(类型) + 内容长度
   uint32_t total_len = 4 + 1 + content.size();
   // 转成大端（网络字节序）
   uint32_t net_len = htonl(total_len);
   // 拼接：长度头  +类型  +内容
   packet.append((char *)&net_len, 4); // 4 字节  长度
   packet.append((char *)&type, 1);    // 1字节  类型
   packet.append(content);             // 内容
   return packet;
}
// ============ 解包函数 ============
inline bool decode_packet(std::string &recv_buf, uint8_t &out_type, std::string &out_payload)
{
   // 检查头部是否完整
   if (recv_buf.size() < 4)
   {
      return false;
   }
   // 读取总长度   转成主机字节序
   uint32_t pkg_len;
   memcpy(&pkg_len, recv_buf.data(), 4);
   pkg_len = ntohl(pkg_len);

   // 防御非法长度
   const uint32_t MAX_PKG_LEN = 64 * 1024;  // 64KB，单包最大限制

   // 检查整个包是否完整
  if (pkg_len < 5 || pkg_len > MAX_PKG_LEN) {
    recv_buf.erase(0, 4);
    return false;
}if (recv_buf.size() < pkg_len) return false;

   // 提取数据
   out_type = recv_buf[4]; // 提取类型：长度头占 4 字节，类型在第 5 个字节（下标 4）

   out_payload.assign(recv_buf.data() + 5, pkg_len - 5); // 提取 payload：跳过 5 字节头部
   // 删除已处理的包
   recv_buf.erase(0, pkg_len);
   // 成功解析出一个完整包
   return true;
}

#endif