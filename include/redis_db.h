#pragma once
#include "redis_dic.h"
#include "utils.h"

namespace rockin {
class RedisCmd;
enum ValueType {
  TypeString = 1,
  TypeInteger = 2,
  TypeList = 4,
  TypeHash = 8,
  TypeSet = 16,
  TypeZSet = 32,
};

class RedisDB;
class RedisObj {
 public:
  friend RedisDB;
  friend RedisDic<RedisObj>;

  RedisObj() {}
  RedisObj(int t, std::shared_ptr<buffer_t> key, std::shared_ptr<void> v)
      : type_(t), key_(key), value_(v) {}
  virtual ~RedisObj() {}

  int type() { return type_; }
  std::shared_ptr<buffer_t> key() { return key_; }
  std::shared_ptr<void> value() { return value_; }

 private:
  int type_;
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
  void Set(std::shared_ptr<buffer_t> key, std::shared_ptr<void> value,
           ValueType type);

  void SetObj(std::shared_ptr<RedisObj> obj, std::shared_ptr<void> value,
              ValueType type);

  // delete by key
  bool Delete(std::shared_ptr<buffer_t> key);

 private:
  RedisDic<RedisObj> dic_;
};

extern bool CheckAndReply(std::shared_ptr<RedisObj> obj,
                          std::shared_ptr<RedisCmd> cmd, int type);

extern void ReplyRedisObj(std::shared_ptr<RedisObj> obj,
                          std::shared_ptr<RedisCmd> cmd);

extern std::shared_ptr<buffer_t> ObjToString(std::shared_ptr<RedisObj> obj);

extern bool ObjToInt64(std::shared_ptr<RedisObj> obj, int64_t &v);

extern bool ObjToDouble(std::shared_ptr<RedisObj> obj, double &v);
}  // namespace rockin
