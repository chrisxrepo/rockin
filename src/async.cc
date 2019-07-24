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
AsyncQueue::AsyncQueue(size_t max_size)
    : max_size_(max_size), cur_size_(0), read_wait_(0), write_wait_(0) {
  // init mutex
  int retcode = uv_mutex_init(&mutex_);
  LOG_IF(FATAL, retcode) << "uv_mutex_init errer:" << GetUvError(retcode);

  // init cond
  retcode = uv_cond_init(&read_cond_);
  LOG_IF(FATAL, retcode) << "uv_cond_init errer:" << GetUvError(retcode);
  retcode = uv_cond_init(&write_cond_);
  LOG_IF(FATAL, retcode) << "uv_cond_init errer:" << GetUvError(retcode);

  // init queue
  QUEUE_INIT(&queue_);
}

QUEUE *AsyncQueue::Pop() {
  uv_mutex_lock(&mutex_);
  while (QUEUE_EMPTY(&queue_)) {
    read_wait_++;
    uv_cond_wait(&read_cond_, &mutex_);
    read_wait_--;
  }

  QUEUE *q = QUEUE_HEAD(&queue_);
  QUEUE_REMOVE(q);
  cur_size_--;
  if (write_wait_ > 0) uv_cond_signal(&write_cond_);

  uv_mutex_unlock(&mutex_);
  return q;
}

void AsyncQueue::Push(QUEUE *q) {
  uv_mutex_lock(&mutex_);
  while (cur_size_ >= max_size_) {
    write_wait_++;
    uv_cond_wait(&write_cond_, &mutex_);
    write_wait_--;
  }

  QUEUE_INSERT_TAIL(&queue_, q);
  cur_size_++;
  if (read_wait_ > 0) uv_cond_signal(&read_cond_);
  uv_mutex_unlock(&mutex_);
}

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