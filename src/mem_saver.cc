#include "mem_saver.h"
#include <glog/logging.h>
#include <stdlib.h>
#include <mutex>
#include "dic_table.h"
#include "siphash.h"
#include "utils.h"

namespace rockin {
namespace {
std::once_flag once_flag;
MemSaver *g_data;
};  // namespace

MemSaver *MemSaver::Default() {
  std::call_once(once_flag, []() { g_data = new MemSaver(); });
  return g_data;
}

static uint64_t obj_key_hash(std::shared_ptr<object_t> obj) {
  return rockin::Hash((const uint8_t *)obj->key->data, obj->key->len);
}

static bool obj_key_equal(std::shared_ptr<object_t> a,
                          std::shared_ptr<object_t> b) {
  if ((a == nullptr || a->key == nullptr) &&
      (b == nullptr || b->key == nullptr))
    return true;

  if (a == nullptr || a->key == nullptr || b == nullptr || b->key == nullptr)
    return false;

  if (a->key->len != b->key->len) return false;
  for (size_t i = 0; i < a->key->len; i++) {
    if (*((char *)(a->key->data) + i) != *((char *)(b->key->data) + i)) {
      return false;
    }
  }
  return true;
}

static int obj_key_compare(std::shared_ptr<object_t> a,
                           std::shared_ptr<object_t> b) {
  if ((a == nullptr || a->key == nullptr) &&
      (b == nullptr || b->key == nullptr))
    return 0;
  if (a == nullptr || a->key == nullptr) return -1;
  if (b == nullptr || b->key == nullptr) return 1;

  for (size_t i = 0; i < a->key->len && i < b->key->len; i++) {
    uint8_t ac = *((uint8_t *)(a->key->data) + i);
    uint8_t bc = *((uint8_t *)(b->key->data) + i);
    if (ac > bc) return 1;
    if (ac < bc) return -1;
  }

  if (a->key->len > b->key->len) return 1;
  if (a->key->len < b->key->len) return -1;
  return 0;
}

MemSaver::MemSaver() : thread_num_(0) {}

MemSaver::~MemSaver() {}

struct MemAsyncQueue : AsyncQueue {
  std::shared_ptr<DicTable<object_t>> db;
};

bool MemSaver::Init(size_t thread_num) {
  thread_num_ = thread_num;

  for (size_t i = 0; i < thread_num; i++) {
    MemAsyncQueue *async = new MemAsyncQueue();

    // memory database
    async->db =
        std::make_shared<DicTable<object_t>>(obj_key_equal, obj_key_hash);

    asyncs_.push_back(async);
  }

  return this->InitAsync(thread_num);
}

void MemSaver::AsyncWork(int idx) {
  MemAsyncQueue *async = asyncs_[idx];

  while (true) {
    uv_mutex_lock(&async->mutex);
    while (QUEUE_EMPTY(&async->queue)) {
      uv_cond_wait(&async->cond, &async->mutex);
    }

    QUEUE *q = QUEUE_HEAD(&async->queue);
    QUEUE_REMOVE(q);
    uv_mutex_unlock(&async->mutex);

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
  MemAsyncQueue *async = asyncs_[idx];
  uv_mutex_lock(&async->mutex);
  QUEUE_INSERT_TAIL(&async->queue, q);
  uv_cond_signal(&async->cond);
  uv_mutex_unlock(&async->mutex);
}

struct GetObjHelper {
  int idx;
  BufPtr key;
  ObjPtr obj;
  std::function<void(BufPtr, ObjPtr)> callback;
};

// get obj from memsaver
void MemSaver::GetObj(uv_loop_t *loop, BufPtr key,
                      std::function<void(BufPtr, ObjPtr)> callback) {
  if (loop == nullptr || key == nullptr || thread_num_ == 0) return;
  int index = rockin::Hash(key->data, key->len) % thread_num_;

  GetObjHelper *helper = new GetObjHelper();
  helper->idx = index;
  helper->key = key;
  helper->callback = callback;

  uv_work_t *req = (uv_work_t *)malloc(sizeof(uv_work_t));
  req->data = helper;

  this->AsyncQueueWork(
      index, loop, req,
      [](uv_work_t *req) {
        GetObjHelper *helper = (GetObjHelper *)req->data;
        MemAsyncQueue *async = MemSaver::Default()->asyncs_[helper->idx];
        auto *node = async->db->Get(rockin::make_object(helper->key));
        if (node != nullptr) helper->obj = node->data;
      },
      [](uv_work_t *req, int status) {
        GetObjHelper *helper = (GetObjHelper *)req->data;
        helper->callback(helper->key, helper->obj);
        delete helper;
        free(req);
      });
}

// get objs from memsaver
void MemSaver::GetObj(uv_loop_t *loop, BufPtrs keys,
                      std::function<void(BufPtrs, ObjPtrs)> callback) {
  //
}

struct InsertObjHelper {
  int idx;
  ObjPtr obj;
  std::function<void(ObjPtr)> callback;
};
// insert obj to memsaver
void MemSaver::InsertObj(uv_loop_t *loop, ObjPtr obj,
                         std::function<void(ObjPtr)> callback) {
  if (thread_num_ == 0) return;
  int index = rockin::Hash(obj->key->data, obj->key->len) % thread_num_;

  InsertObjHelper *helper = new InsertObjHelper();
  helper->idx = index;
  helper->obj = obj;
  helper->callback = callback;

  uv_work_t *req = (uv_work_t *)malloc(sizeof(uv_work_t));
  req->data = helper;

  this->AsyncQueueWork(
      index, loop, req,
      [](uv_work_t *req) {
        InsertObjHelper *helper = (InsertObjHelper *)req->data;
        MemAsyncQueue *async = MemSaver::Default()->asyncs_[helper->idx];

        DicTable<object_t>::Node *node = new DicTable<object_t>::Node();
        node->data = helper->obj;
        node->next = nullptr;
        async->db->Insert(node);
      },
      [](uv_work_t *req, int status) {
        InsertObjHelper *helper = (InsertObjHelper *)req->data;
        helper->callback(helper->obj);
        delete helper;
        free(req);
      });
}

// insert obj to memsaver
void MemSaver::InsertObj(uv_loop_t *loop, ObjPtrs obj,
                         std::function<void(ObjPtrs)> callback) {
  //
}

}  // namespace rockin
