#pragma once
#include "redis_dic.h"
#include "utils.h"

namespace rockin {
class RedisCmd;

class RedisDB;
class RedisObj {
 public:
  enum ValueType {
    String = 1,
    List = 2,
    Hash = 4,
    Set = 8,
    ZSet = 16,
  };

  friend RedisDB;
  friend RedisDic<RedisObj>;

  RedisObj() {}
  RedisObj(int type, int encode, std::shared_ptr<buffer_t> key,
           std::shared_ptr<void> v)
      : type_(type), encode_(encode), key_(key), value_(v) {}
  virtual ~RedisObj() {}

  int type() { return type_; }
  int encode() { return encode_; }
  std::shared_ptr<buffer_t> key() { return key_; }
  std::shared_ptr<void> value() { return value_; }

 private:
  unsigned char type_;
  unsigned char encode_;
  std::shared_ptr<buffer_t> key_;
  std::shared_ptr<void> value_;
  std::shared_ptr<RedisObj> next_;
};

class RedisDB {
 public:
  RedisDB();
  ~RedisDB();

  // get
  std::shared_ptr<RedisObj> Get(std::shared_ptr<buffer_t> key);

  // get if nil reply
  std::shared_ptr<RedisObj> GetReplyNil(std::shared_ptr<buffer_t> key,
                                        std::shared_ptr<RedisCmd> cmd);

  // set
  std::shared_ptr<RedisObj> Set(std::shared_ptr<buffer_t> key,
                                std::shared_ptr<void> value, unsigned char type,
                                unsigned char encode);

  void SetObj(std::shared_ptr<RedisObj> obj, std::shared_ptr<void> value,
              unsigned char type, unsigned char encode);

  // delete by key
  bool Delete(std::shared_ptr<buffer_t> key);

 private:
  RedisDic<RedisObj> dic_;
};

extern bool CheckAndReply(std::shared_ptr<RedisObj> obj,
                          std::shared_ptr<RedisCmd> cmd, int type);

extern void ReplyRedisObj(std::shared_ptr<RedisObj> obj,
                          std::shared_ptr<RedisCmd> cmd);

}  // namespace rockin
