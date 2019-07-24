#pragma once
#include <glog/logging.h>
#include <uv.h>
#include <vector>
#include "queue.h"
#include "utils.h"

namespace rockin {

class AsyncQueue {
 public:
  /*
   * max_size queue max size
   */
  AsyncQueue(size_t max_size);

  /*
   * pop element form queue
   * if queue empry, wait...
   */
  QUEUE *Pop();

  /*
   * push element to queue
   *if queue full, wait...
   */
  void Push(QUEUE *q);

 private:
  QUEUE queue_;
  uv_mutex_t mutex_;
  uv_cond_t read_cond_, write_cond_;
  size_t max_size_, cur_size_;
  int read_wait_, write_wait_;
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