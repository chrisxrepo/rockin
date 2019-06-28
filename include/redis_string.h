#pragma once
#include <stdint.h>
#include <string.h>
#include <iostream>
#include "utils.h"

namespace rockin {
class Conn;
class RedisCmd;

class RedisString {
 public:
  enum Type {
    Raw = 1,
    Int = 2,
  };

  RedisString() {}

  RedisString(int64_t value) { SetIntValue(value); }

  RedisString(std::shared_ptr<buffer_t> value) : value_(value) {}

  void SetValue(std::shared_ptr<buffer_t> v) { value_ = v; }
  std::shared_ptr<buffer_t> Value() { return value_; }

  void SetIntValue(int64_t value) {
    if (value_ == nullptr || value_->len < sizeof(int64_t))
      value_ = make_buffer(sizeof(int64_t));
    *((int64_t *)value_->data) = value;
  }

  int64_t IntValue() {
    if (value_ == nullptr || value_->len < sizeof(int64_t)) return 0;
    return *((int64_t *)value_->data);
  }

 private:
  std::shared_ptr<buffer_t> value_;
};

// GET key
extern void GetCommand(std::shared_ptr<RedisCmd> cmd);

// SET key value
extern void SetCommand(std::shared_ptr<RedisCmd> cmd);

// APPEND key value
extern void AppendCommand(std::shared_ptr<RedisCmd> cmd);

// GETSET key value
extern void GetSetCommand(std::shared_ptr<RedisCmd> cmd);

// MGET key1 ....
extern void MGetCommand(std::shared_ptr<RedisCmd> cmd);

// MSET key1 value1 ....
extern void MSetCommand(std::shared_ptr<RedisCmd> cmd);

// INCR key
extern void IncrCommand(std::shared_ptr<RedisCmd> cmd);

// INCRBY key value
extern void IncrbyCommand(std::shared_ptr<RedisCmd> cmd);

// DECR key
extern void DecrCommand(std::shared_ptr<RedisCmd> cmd);

// DECR key value
extern void DecrbyCommand(std::shared_ptr<RedisCmd> cmd);

// SETBIT key offset value
extern void SetBitCommand(std::shared_ptr<RedisCmd> cmd);

// GETBIT key offset
extern void GetBitCommand(std::shared_ptr<RedisCmd> cmd);

// BITCOUNT key [start end]
extern void BitCountCommand(std::shared_ptr<RedisCmd> cmd);

// BITOP AND destkey srckey1 srckey2 srckey3 ... srckeyN
// BITOP OR destkey srckey1 srckey2 srckey3... srckeyN
// BITOP XOR destkey srckey1 srckey2 srckey3... srckeyN
// BITOP NOT destkey srckey
extern void BitOpCommand(std::shared_ptr<RedisCmd> cmd);

// BITPOS key bit[start][end]
extern void BitOpsCommand(std::shared_ptr<RedisCmd> cmd);

}  // namespace rockin
