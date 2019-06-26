#pragma once
#include <stdint.h>
#include <iostream>
#include "utils.h"

namespace rockin {
class Conn;
class RedisCmd;

class RedisInteger {
 public:
  RedisInteger() : value_(0) {}
  RedisInteger(int64_t v) : value_(v) {}

  void Add(int n) { value_ += n; }
  void SetValue(int64_t v) { value_ = v; }

  int64_t Value() { return value_; }
  // std::string ToString() override { return Int64ToString(value_); }

 private:
  int64_t value_;
};

class RedisString {
 public:
  RedisString() {}
  RedisString(const std::string &value) : value_(value) {}
  RedisString(std::string &&value) : value_(value) {}

  void SetValue(const std::string &v) { value_.assign(v); }
  void SetValue(std::string &&v) { value_.assign(v); }
  void Append(const std::string &v) { value_.append(v); }

  const std::string &Value() { return value_; }
  //  std::string ToString() override { return value_; }

 private:
  std::string value_;
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

// DECRBYFLOAT key value
extern void IncrbyFloatCommand(std::shared_ptr<RedisCmd> cmd);

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
