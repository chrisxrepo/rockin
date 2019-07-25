#pragma once
#include <uv.h>
#include "async.h"
#include "mem_alloc.h"

namespace rockin {
struct MemAsyncQueue;

class MemSaver {
 public:
  static MemSaver *Default();

  MemSaver();
  ~MemSaver();

  bool Init();

  // get obj from memsaver
  ObjPtr GetObj(BufPtr key);

  // get objs from memsaver
  ObjPtrs GetObj(BufPtrs keys);

  // insert obj to memsaver
  void InsertObj(ObjPtr obj);

  // insert obj to memsaver
  void InsertObj(ObjPtrs obj);

  // update expire
  void UpdateExpire(ObjPtr obj, uint64_t expire_ms);

 private:
  uv_key_t key_;
};

}  // namespace rockin