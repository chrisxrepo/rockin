#pragma once
#include <iostream>
#include <unordered_map>
#include <vector>
#include "byte_buf.h"
#include "utils.h"

namespace rockin {
class Conn;
class RedisCmd : public std::enable_shared_from_this<RedisCmd> {
 public:
  static void InitHandle();

  RedisCmd(std::shared_ptr<Conn> conn);

  bool Parse(ByteBuf &buf);
  void Handle();

  void ReplyNil();
  void ReplyTypeWaring();
  void ReplyError(std::string &&errstr);
  void ReplyString(std::string &&str);
  void ReplyInteger(int64_t num);
  void ReplyBulk(std::string &&str);
  void ReplyArray(std::vector<std::string> &values);
  void ReplyArray(std::vector<std::string> &values, std::vector<bool> &exists);

  const std::string &Cmd();
  std::vector<std::string> &Args();

  // redisCmd string
  std::string ToString();

 private:
  bool ParseMultiCommand(ByteBuf &buf);
  bool ParseInlineCommand(ByteBuf &buf);

  // close client conn
  void CloseConn();

 private:
  std::weak_ptr<Conn> conn_;
  std::vector<std::string> args_;
  int mbulk_;
};

}  // namespace rockin
