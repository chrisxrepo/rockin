#pragma once
#include <atomic>
#include <iostream>
#include "utils.h"

namespace rockin {
class Conn;
class RedisCmd;

struct MultiResult {
  std::atomic<uint32_t> cnt;
  std::vector<std::string> values;
  std::vector<bool> exists;
  std::atomic<int64_t> int_value;

  MultiResult(uint32_t cnt_)
      : cnt(cnt_), values(cnt_), exists(cnt_), int_value(0) {}
};

// command
extern void CommandCommand(std::shared_ptr<RedisCmd> cmd);

// ping
extern void PingCommand(std::shared_ptr<RedisCmd> cmd);

// info
extern void InfoCommand(std::shared_ptr<RedisCmd> cmd);

// del key1 ...
extern void DelCommand(std::shared_ptr<RedisCmd> cmd);

}  // namespace rockin
