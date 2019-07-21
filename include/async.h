#pragma once
#include <uv.h>
#include <vector>
#include "queue.h"

namespace rockin {
struct AsyncQueue {
  QUEUE queue;
  uv_cond_t cond;
  uv_mutex_t mutex;
};

class Async {
 public:
  Async();
  virtual ~Async();

  bool InitAsync(size_t thread_num);
  void WaitStop();

 protected:
  int AsyncQueueWork(int idx, uv_loop_t *loop, uv_work_t *req,
                     uv_work_cb work_cb, uv_after_work_cb after_work_cb);

 private:
  virtual void AsyncWork(int idx) = 0;
  virtual void PostWork(int idx, QUEUE *q) = 0;

 private:
  uv_sem_t start_sem_;
  std::vector<uv_thread_t> threads_;
};
}  // namespace rockin