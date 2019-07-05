#pragma once
#include <atomic>
#include <iostream>
#include "utils.h"

#define DBNum 16

namespace rockin {
class RockinConn;
class RedisArgs;

struct MultiResult {
  std::atomic<uint32_t> cnt;
  std::atomic<int64_t> int_value;
  std::vector<std::shared_ptr<buffer_t>> str_values;

  MultiResult(uint32_t cnt_) : cnt(cnt_), int_value(0), str_values(cnt_) {}
};

// command
extern void CommandCommand(std::shared_ptr<RedisArgs> cmd);

// ping
extern void PingCommand(std::shared_ptr<RedisArgs> cmd);

// info
extern void InfoCommand(std::shared_ptr<RedisArgs> cmd);

// del key1 ...
extern void DelCommand(std::shared_ptr<RedisArgs> cmd);

// select dbnum
extern void SelectCommand(std::shared_ptr<RedisArgs> cmd);

// FLUSHDB
extern void FlushDBCommand(std::shared_ptr<RedisArgs> cmd);

// FLUSHALL
extern void FlushAllCommand(std::shared_ptr<RedisArgs> cmd);

}  // namespace rockin
