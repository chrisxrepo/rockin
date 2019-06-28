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
