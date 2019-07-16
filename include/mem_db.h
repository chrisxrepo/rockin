#pragma once
#include <uv.h>
#include <vector>
#include "dic_table.h"
#include "rockin_alloc.h"
#include "siphash.h"
#include "skip_list.h"
#include "utils.h"

#define EXPIRE_SKIPLIST_LEVEL 3

#define OBJ_STRING(obj) std::static_pointer_cast<membuf_t>(obj->value)
#define OBJ_SET_VALUE(o, v, t, e) \
  do {                            \
    (o)->type = (t);              \
    (o)->encode = (e);            \
    (o)->value = (v);             \
  } while (0)

#define BUF_INT64(v) (*((int64_t *)v->data))

namespace rockin {
struct ExpireNode;

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
  uint64_t expire;
  MemPtr key;
  std::shared_ptr<void> value;
  std::shared_ptr<MemObj> next;

  MemObj()
      : type(0),
        encode(0),
        version(0),
        expire(0),
        key(nullptr),
        value(nullptr),
        next(nullptr) {}

  MemObj(MemPtr key_)
      : type(0),
        encode(0),
        version(0),
        expire(0),
        key(key_),
        value(nullptr),
        next(nullptr) {}

  size_t Size() { return sizeof(MemObj); }
};

class MemDB {
 public:
  MemDB();
  ~MemDB();

  DicTable<MemObj>::Node *GetNode(int dbindex, MemPtr key);

  std::shared_ptr<MemObj> Get(int dbindex, MemPtr key);

  void Insert(int dbindex, std::shared_ptr<MemObj> obj);

  // delete by key
  bool Delete(int dbindex, MemPtr key);

  // add key expire
  void UpdateExpire(std::shared_ptr<MemObj> obj, uint64_t expire_ms);

  // flush db
  void FlushDB(int dbindex);

  void RehashTimer(uint64_t time);
  void ExpireTimer(uint64_t time);

 private:
  bool DoRehash();

 private:
  std::vector<std::shared_ptr<DicTable<MemObj>>> dics_;
  SkipList<MemObj, EXPIRE_SKIPLIST_LEVEL> *expire_list_;
};

extern MemPtr GenString(MemPtr value, int encode);

extern bool GenInt64(MemPtr str, int encode, int64_t &v);

extern bool CheckAndReply(std::shared_ptr<MemObj> obj,
                          std::shared_ptr<RockinConn> conn, int type);

}  // namespace rockin
