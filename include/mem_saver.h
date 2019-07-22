#pragma once
#include <functional>
#include "async.h"
#include "mem_alloc.h"

namespace rockin {
struct MemAsyncQueue;

class MemSaver : public Async {
 public:
  static MemSaver *Default();

  MemSaver();
  ~MemSaver();

  bool Init(size_t thread_num);

  // get obj from memsaver
  void GetObj(uv_loop_t *loop, BufPtr key,
              std::function<void(BufPtr, ObjPtr)> callback);

  // get objs from memsaver
  void GetObj(uv_loop_t *loop, BufPtrs keys,
              std::function<void(BufPtrs, ObjPtrs)> callback);

  // insert obj to memsaver
  void InsertObj(uv_loop_t *loop, ObjPtr obj,
                 std::function<void(ObjPtr)> callback);

  // insert obj to memsaver
  void InsertObj(uv_loop_t *loop, ObjPtrs obj,
                 std::function<void(ObjPtrs)> callback);

 private:
  void AsyncWork(int idx) override;
  void PostWork(int idx, QUEUE *q) override;

 private:
  size_t thread_num_;
  std::vector<MemAsyncQueue *> asyncs_;
};
}  // namespace rockin