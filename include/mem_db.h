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

class MemDB;
class CmdArgs;
class RockinConn;

struct MemObj {
  uint8_t type;
  uint8_t encode;
  uint16_t version;
  uint32_t ttl;
  std::shared_ptr<membuf_t> key;
  std::shared_ptr<void> value;
  std::shared_ptr<MemObj> next;
};

class MemDB {
 public:
  MemDB();
  ~MemDB();

  // get
  std::shared_ptr<MemObj> Get(int dbindex, std::shared_ptr<membuf_t> key);

  // get if nil reply
  std::shared_ptr<MemObj> GetReplyNil(int dbindex,
                                      std::shared_ptr<membuf_t> key,
                                      std::shared_ptr<RockinConn> conn);

  // set
  std::shared_ptr<MemObj> Set(int dbindex, std::shared_ptr<membuf_t> key,
                              std::shared_ptr<void> value, unsigned char type,
                              unsigned char encode);

  // delete by key
  bool Delete(int dbindex, std::shared_ptr<membuf_t> key);

  // flush db
  void FlushDB(int dbindex);

 private:
  std::vector<std::shared_ptr<RedisDic<MemObj>>> dics_;
};

extern std::shared_ptr<membuf_t> GenString(std::shared_ptr<membuf_t> value,
                                           int encode);

extern bool GenInt64(std::shared_ptr<membuf_t> str, int encode, int64_t &v);

extern bool CheckAndReply(std::shared_ptr<MemObj> obj,
                          std::shared_ptr<RockinConn> conn, int type);

extern void ReplyMemObj(std::shared_ptr<MemObj> obj,
                        std::shared_ptr<RockinConn> conn);

}  // namespace rockin
