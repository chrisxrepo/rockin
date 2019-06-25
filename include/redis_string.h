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

// get key
extern void GetCommand(std::shared_ptr<RedisCmd> cmd);

// set key value
extern void SetCommand(std::shared_ptr<RedisCmd> cmd);

// append key value
extern void AppendCommand(std::shared_ptr<RedisCmd> cmd);

// getset key value
extern void GetSetCommand(std::shared_ptr<RedisCmd> cmd);

// mget key1 ....
extern void MGetCommand(std::shared_ptr<RedisCmd> cmd);

// mset key1 value1 ....
extern void MSetCommand(std::shared_ptr<RedisCmd> cmd);

// incr key
extern void IncrCommand(std::shared_ptr<RedisCmd> cmd);

// incrby key value
extern void IncrbyCommand(std::shared_ptr<RedisCmd> cmd);

// decr key
extern void DecrCommand(std::shared_ptr<RedisCmd> cmd);

// decr key value
extern void DecrbyCommand(std::shared_ptr<RedisCmd> cmd);

// decr key value
extern void IncrbyFloatCommand(std::shared_ptr<RedisCmd> cmd);

}  // namespace rockin
