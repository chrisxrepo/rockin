#include "mem_saver.h"
#include <glog/logging.h>
#include <stdlib.h>
#include <mutex>
#include "utils.h"

namespace rockin {
namespace {
std::once_flag once_flag;
MemSaver *g_data;
};  // namespace

struct work_data {
  uv_cond_t cond;
  uv_mutex_t mutex;
  QUEUE queue;
};

MemSaver *MemSaver::Default() {
  std::call_once(once_flag, []() { g_data = new MemSaver(); });
  return g_data;
}

MemSaver::MemSaver() {}

MemSaver::~MemSaver() {}

bool MemSaver::Init(size_t thread_num) {
  for (size_t i = 0; i < thread_num; i++) {
    work_data *data = new work_data();
    int retcode = uv_mutex_init(&data->mutex);
    LOG_IF(FATAL, retcode) << "uv_mutex_init errer:" << GetUvError(retcode);

    retcode = uv_cond_init(&data->cond);
    LOG_IF(FATAL, retcode) << "uv_cond_init errer:" << GetUvError(retcode);

    QUEUE_INIT(&data->queue);
    work_datas_.push_back(data);
  }

  return this->InitAsync(thread_num);
}

void MemSaver::AsyncWork(int idx) {
  work_data *data = work_datas_[idx];

  while (true) {
    uv_mutex_lock(&data->mutex);
    while (QUEUE_EMPTY(&data->queue)) {
      uv_cond_wait(&data->cond, &data->mutex);
    }

    QUEUE *q = QUEUE_HEAD(&data->queue);
    QUEUE_REMOVE(q);
    uv_mutex_unlock(&data->mutex);

    uv__work *w = QUEUE_DATA(q, struct uv__work, wq);
    w->work(w);

    // async done
    uv_mutex_lock(&w->loop->wq_mutex);
    w->work = NULL;
    QUEUE_INSERT_TAIL(&w->loop->wq, &w->wq);
    uv_async_send(&w->loop->wq_async);
    uv_mutex_unlock(&w->loop->wq_mutex);
  }
}

void MemSaver::PostWork(int idx, QUEUE *q) {
  work_data *data = work_datas_[idx];
  uv_mutex_lock(&data->mutex);
  QUEUE_INSERT_TAIL(&data->queue, q);
  uv_cond_signal(&data->cond);
  uv_mutex_unlock(&data->mutex);
}

struct get_obj_helper_data {
  BufPtr key;
  BufPtr value;
  std::function<void(ObjPtr)> callback;
};

// get obj from memsaver
void MemSaver::GetObj(uv_loop_t *loop, BufPtr key,
                      std::function<void(ObjPtr)> callback) {
  get_obj_helper_data *help_data = new get_obj_helper_data();
  help_data->key = key;
  help_data->callback = callback;

  uv_work_t *req = (uv_work_t *)malloc(sizeof(uv_work_t));
  req->data = help_data;

  this->AsyncQueueWork(
      std::rand() % work_datas_.size(), loop, req,
      [](uv_work_t *req) { LOG(INFO) << "work doing:" << uv_thread_self(); },
      [](uv_work_t *req, int status) {
        LOG(INFO) << "work done:" << uv_thread_self();
      });
}

// get objs from memsaver
void MemSaver::GetObj(uv_loop_t *loop, BufPtrs keys,
                      std::function<void(ObjPtrs)> callback) {
  //
}

// insert obj to memsaver
void MemSaver::InsertObj(uv_loop_t *loop, ObjPtr obj,
                         std::function<void(ObjPtr)> callback) {
  //
}

// insert obj to memsaver
void MemSaver::InsertObj(uv_loop_t *loop, ObjPtrs obj,
                         std::function<void(ObjPtrs)> callback) {
  //
}

}  // namespace rockin
