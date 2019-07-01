#pragma once
#include "redis_dic.h"
#include "utils.h"

#define OBJ_STRING(obj) std::static_pointer_cast<buffer_t>(obj->value)
#define OBJ_SET_VALUE(o, v, t, e) \
  do {                            \
    o->type = t;                  \
    o->encode = e;                \
    o->value = v;                 \
  } while (0)

#define BUF_INT64(v) (*((int64_t *)v->data))

namespace rockin {
enum ValueType {
  Type_String = 1,
  Type_List = 2,
  Type_Hash = 4,
  Type_Set = 8,
  Type_ZSet = 16,
};

enum EncodeType {
  Encode_Raw = 1,
  Encode_Int = 2,
};

class RedisCmd;
class RedisDB;

struct RedisObj {
  unsigned char type;
  unsigned char encode;
  std::shared_ptr<buffer_t> key;
  std::shared_ptr<void> value;
  std::shared_ptr<RedisObj> next;
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

  // delete by key
  bool Delete(std::shared_ptr<buffer_t> key);

 private:
  RedisDic<RedisObj> dic_;
};

extern std::shared_ptr<buffer_t> GenString(std::shared_ptr<buffer_t> value,
                                           int encode);

extern bool GenInt64(std::shared_ptr<buffer_t> str, int encode, int64_t &v);

extern bool CheckAndReply(std::shared_ptr<RedisObj> obj,
                          std::shared_ptr<RedisCmd> cmd, int type);

extern void ReplyRedisObj(std::shared_ptr<RedisObj> obj,
                          std::shared_ptr<RedisCmd> cmd);

}  // namespace rockin
