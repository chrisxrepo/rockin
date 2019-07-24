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
   * queue_num the number of queues
   * max_size the size of pequeue
   */
  AsyncQueue(size_t queue_num, size_t max_size);

  /*
   * pop element form queues[0]
   * if queues[0] empry, wait...
   */
  QUEUE *Pop();

  /*
   * pop elements from queues
   * if queues empry, wait...
   * @max_num the max number pop elements
   * @idx return queue idx
   * @return pop elements
   */
  std::vector<QUEUE *> Pop(int &idx, size_t max_num);

  /*
   * push element to queues[0]
   *if queues[0] full, wait...
   */
  void Push(QUEUE *q);

  /*
   * push element to queues[idx]
   *if queue full, wait...
   */
  void Push(QUEUE *q, int idx);

 private:
  inline bool QueuesEmpty();
  inline int NextNoEmptyQueue();

 private:
  size_t queue_num_;
  QUEUE *queues_;
  uv_mutex_t mutex_;
  uv_cond_t *conds_;
  int *waits_;
  size_t *sizes_;
  size_t max_size_;
  uint64_t snum_;
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