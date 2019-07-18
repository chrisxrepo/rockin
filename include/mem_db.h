#pragma once
#include <uv.h>
#include <vector>
#include "dic_table.h"
#include "mem_alloc.h"
#include "siphash.h"
#include "skip_list.h"
#include "utils.h"

#define EXPIRE_SKIPLIST_LEVEL 3

#define OBJ_STRING(obj) std::static_pointer_cast<buffer_t>(obj->value)
#define OBJ_SET_VALUE(o, v, t, e) \
  do {                            \
    (o)->type = (t);              \
    (o)->encode = (e);            \
    (o)->value = (v);             \
  } while (0)

#define BUF_INT64(v) (*((int64_t *)v->data))

namespace rockin {
struct ExpireNode;
class RockinConn;

class MemDB {
 public:
  MemDB();
  ~MemDB();

  DicTable<object_t>::Node *GetNode(int dbindex, BufPtr key);

  std::shared_ptr<object_t> Get(int dbindex, BufPtr key);

  void Insert(int dbindex, std::shared_ptr<object_t> obj);

  // delete by key
  bool Delete(int dbindex, BufPtr key);

  // add key expire
  void UpdateExpire(int dbindex, std::shared_ptr<object_t> obj,
                    uint64_t expire_ms);

  // flush db
  void FlushDB(int dbindex);

  void RehashTimer(uint64_t time);
  void ExpireTimer(uint64_t time);

 private:
  std::vector<std::shared_ptr<DicTable<object_t>>> dics_;
  std::vector<std::shared_ptr<SkipList<object_t, EXPIRE_SKIPLIST_LEVEL>>>
      expires_;
};

extern BufPtr GenString(BufPtr value, int encode);

extern bool GenInt64(BufPtr str, int encode, int64_t &v);

extern bool CheckAndReply(std::shared_ptr<object_t> obj,
                          std::shared_ptr<RockinConn> conn, int type);

}  // namespace rockin
