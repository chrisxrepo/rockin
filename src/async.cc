#include "async.h"
#include <assert.h>
#include <glog/logging.h>
#include "utils.h"

#define uv__has_active_reqs(loop) ((loop)->active_reqs.count > 0)

#define uv__req_register(loop, req) \
  do {                              \
    (loop)->active_reqs.count++;    \
  } while (0)

#define uv__req_unregister(loop, req)  \
  do {                                 \
    assert(uv__has_active_reqs(loop)); \
    (loop)->active_reqs.count--;       \
  } while (0)

#if defined(_WIN32)
#define UV_REQ_INIT(req, typ)                                    \
  do {                                                           \
    (req)->type = (typ);                                         \
    (req)->u.io.overlapped.Internal = 0; /* SET_REQ_SUCCESS() */ \
  } while (0)
#else
#define UV_REQ_INIT(req, typ) \
  do {                        \
    (req)->type = (typ);      \
  } while (0)
#endif

#define uv__req_init(loop, req, typ) \
  do {                               \
    UV_REQ_INIT(req, typ);           \
    uv__req_register(loop, req);     \
  } while (0)

namespace rockin {
AsyncQueue::AsyncQueue(size_t max_size) : AsyncQueue(1, max_size) {}

AsyncQueue::AsyncQueue(size_t queue_num, size_t max_size)
    : queue_num_(queue_num), max_size_(max_size), snum_(0) {
  // init mutex
  int retcode = uv_mutex_init(&mutex_);
  LOG_IF(FATAL, retcode) << "uv_mutex_init errer:" << GetUvError(retcode);

  /*
   * [0 - queue_num) for write
   * queue_num for read
   */
  if (queue_num_ < 1) queue_num_ = 1;

  sizes_ = (size_t *)malloc(sizeof(size_t) * queue_num_);
  memset(sizes_, 0, sizeof(size_t) * queue_num_);
  queues_ = (QUEUE *)malloc(sizeof(QUEUE) * queue_num_);
  for (size_t i = 0; i < queue_num_; i++) QUEUE_INIT(queues_ + i);

  waits_ = (int *)malloc(sizeof(int) * (queue_num_ + 1));
  memset(waits_, 0, sizeof(int) * (queue_num_ + 1));
  conds_ = (uv_cond_t *)malloc(sizeof(uv_cond_t) * (queue_num_ + 1));

  for (size_t i = 0; i < queue_num_ + 1; i++) {
    retcode = uv_cond_init(conds_ + i);
    LOG_IF(FATAL, retcode) << "uv_cond_init errer:" << GetUvError(retcode);
  }
}

QUEUE *AsyncQueue::Pop() {
  uv_mutex_lock(&mutex_);
  while (QueuesEmpty()) {
    (*(waits_ + queue_num_))++;
    uv_cond_wait(conds_ + queue_num_, &mutex_);
    (*(waits_ + queue_num_))--;
  }

  int idx = NextNoEmptyQueue();
  QUEUE *q = QUEUE_HEAD(queues_ + idx);
  QUEUE_REMOVE(q);
  (*(sizes_ + idx))--;

  if (*(waits_ + idx) > 0) uv_cond_signal(conds_ + idx);

  uv_mutex_unlock(&mutex_);
  return q;
}

std::vector<QUEUE *> AsyncQueue::Pop(int &idx, size_t max_num) {
  uv_mutex_lock(&mutex_);
  while (QueuesEmpty()) {
    (*(waits_ + queue_num_))++;
    uv_cond_wait(conds_ + queue_num_, &mutex_);
    (*(waits_ + queue_num_))--;
  }

  int num = 0;
  std::vector<QUEUE *> qs;
  idx = NextNoEmptyQueue();
  while (num < max_num && !QUEUE_EMPTY(queues_ + idx)) {
    QUEUE *q = QUEUE_HEAD(queues_ + idx);
    QUEUE_REMOVE(q);
    qs.push_back(q);

    (*(sizes_ + idx))--;
    num++;
  }

  if (*(waits_ + idx) > 0) uv_cond_signal(conds_ + idx);

  uv_mutex_unlock(&mutex_);
  return qs;
}

void AsyncQueue::Push(QUEUE *q) { Push(q, 0); }

void AsyncQueue::Push(QUEUE *q, int idx) {
  if (idx >= queue_num_) return;

  uv_mutex_lock(&mutex_);
  while (*(sizes_ + idx) >= max_size_) {
    (*(waits_ + idx))++;
    uv_cond_wait(conds_ + idx, &mutex_);
    (*(waits_ + idx))--;
  }

  QUEUE_INSERT_TAIL(queues_ + idx, q);
  (*(sizes_ + idx))++;
  if (*(waits_ + queue_num_) > 0) uv_cond_signal(conds_ + queue_num_);

  uv_mutex_unlock(&mutex_);
}

inline bool AsyncQueue::QueuesEmpty() {
  for (size_t i = 0; i < queue_num_; i++) {
    if (!QUEUE_EMPTY(queues_ + i)) return false;
  }
  return true;
}

inline int AsyncQueue::NextNoEmptyQueue() {
  for (size_t i = 0; i < queue_num_; i++) {
    int idx = (snum_++) % queue_num_;
    if (!QUEUE_EMPTY(queues_ + idx)) {
      return idx;
    }
  }
  return 0;
}

/////////////////////////////////////////////////////////////////////////

Async::Async() {
  int retcode = uv_sem_init(&start_sem_, 0);
  LOG_IF(FATAL, retcode) << "uv_sem_init error:" << GetUvError(retcode);
}

Async::~Async() {
  //
  uv_sem_destroy(&start_sem_);
}

bool Async::InitAsync(size_t thread_num) {
  struct _thread_data {
    Async *ptr;
    int idx;
    _thread_data(Async *p, int i) : ptr(p), idx(i) {}
  };

  for (size_t i = 0; i < thread_num; i++) {
    uv_thread_t tid;
    _thread_data *data = new _thread_data(this, i);
    int retcode = uv_thread_create(&tid,
                                   [](void *arg) {
                                     _thread_data *data = (_thread_data *)arg;
                                     uv_sem_post(&data->ptr->start_sem_);
                                     data->ptr->AsyncWork(data->idx);
                                     delete data;
                                   },
                                   data);

    LOG_IF(FATAL, retcode) << "uv_thread_create error:" << GetUvError(retcode);
    threads_.push_back(tid);
  }

  for (size_t i = 0; i < thread_num; i++) {
    uv_sem_wait(&start_sem_);
  }

  return true;
}

static void uv__queue_work(struct uv__work *w) {
  uv_work_t *req = container_of(w, uv_work_t, work_req);

  req->work_cb(req);
}

static void uv__queue_done(struct uv__work *w, int err) {
  uv_work_t *req;

  req = container_of(w, uv_work_t, work_req);
  uv__req_unregister(req->loop, req);

  if (req->after_work_cb == NULL) return;

  req->after_work_cb(req, err);
}

int Async::AsyncQueueWork(int idx, uv_loop_t *loop, uv_work_t *req,
                          uv_work_cb work_cb, uv_after_work_cb after_work_cb) {
  if (loop == nullptr) return -1;

  uv__req_init(loop, req, UV_WORK);
  req->loop = loop;
  req->work_cb = work_cb;
  req->after_work_cb = after_work_cb;
  req->work_req.loop = loop;
  req->work_req.work = uv__queue_work;
  req->work_req.done = uv__queue_done;
  this->PostWork(idx, &req->work_req.wq);
  return 0;
}

void Async::WaitStop() {
  for (size_t i = 0; i < threads_.size(); i++) {
    uv_thread_join(&threads_[i]);
  }
  threads_.clear();
}

}  // namespace rockin