#pragma once
#include <vector>
#include "redis_dic.h"
#include "rockin_alloc.h"
#include "utils.h"

#define OBJ_STRING(obj) std::static_pointer_cast<membuf_t>(obj->value)
#define OBJ_SET_VALUE(o, v, t, e) \
  do {                            \
    o->type = t;                  \
    o->encode = e;                \
    o->value = v;                 \
  } while (0)

#define BUF_INT64(v) (*((int64_t *)v->data))

namespace rockin {
enum ValueType {
  Type_None = 0,
  Type_String = 1,
  Type_List = 2,
  Type_Hash = 4,
  Type_Set = 8,
  Type_ZSet = 16,
};

enum EncodeType {
  Encode_None = 0,
  Encode_Raw = 1,
  Encode_Int = 2,
};

class MemDB;
class CmdArgs;
class RockinConn;

struct MemObj {
  uint8_t type;
  uint8_t encode;
  uint32_t version;
  uint32_t ttl;
  MemPtr key;
  std::shared_ptr<void> value;
  std::shared_ptr<MemObj> next;

  MemObj()
      : type(0),
        encode(0),
        version(0),
        ttl(0),
        key(nullptr),
        value(nullptr),
        next(nullptr) {}

  size_t Size() { return sizeof(MemObj); }
};

class MemDB {
 public:
  MemDB();
  ~MemDB();

  // get
  std::shared_ptr<MemObj> Get(int dbindex, MemPtr key);

  // get if nil reply
  std::shared_ptr<MemObj> GetReplyNil(int dbindex, MemPtr key,
                                      std::shared_ptr<RockinConn> conn);

  // set
  std::shared_ptr<MemObj> Set(int dbindex, MemPtr key,
                              std::shared_ptr<void> value, unsigned char type,
                              unsigned char encode);

  void Insert(int dbindex, std::shared_ptr<MemObj> obj);

  // delete by key
  bool Delete(int dbindex, MemPtr key);

  // flush db
  void FlushDB(int dbindex);

 private:
  std::vector<std::shared_ptr<RedisDic<MemObj>>> dics_;
};

extern MemPtr GenString(MemPtr value, int encode);

extern bool GenInt64(MemPtr str, int encode, int64_t &v);

extern bool CheckAndReply(std::shared_ptr<MemObj> obj,
                          std::shared_ptr<RockinConn> conn, int type);

extern void ReplyMemObj(std::shared_ptr<MemObj> obj,
                        std::shared_ptr<RockinConn> conn);

}  // namespace rockin
