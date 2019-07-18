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

struct work_data {
  std::shared_ptr<DicTable<object_t>> db;

  uv_cond_t cond;
  uv_mutex_t mutex;
  QUEUE queue;
};

bool MemSaver::Init(size_t thread_num) {
  thread_num_ = thread_num;

  for (size_t i = 0; i < thread_num; i++) {
    work_data *data = new work_data();

    // memory database
    data->db =
        std::make_shared<DicTable<object_t>>(obj_key_equal, obj_key_hash);

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

struct get_obj_helper {
  int idx;
  BufPtr key;
  ObjPtr obj;
  std::function<void(ObjPtr)> callback;
};

// get obj from memsaver
void MemSaver::GetObj(uv_loop_t *loop, BufPtr key,
                      std::function<void(ObjPtr)> callback) {
  if (thread_num_ == 0) return;
  int index = rockin::Hash(key->data, key->len) % thread_num_;

  get_obj_helper *help_data = new get_obj_helper();
  help_data->idx = index;
  help_data->key = key;
  help_data->callback = callback;

  uv_work_t *work_req = (uv_work_t *)malloc(sizeof(uv_work_t));
  work_req->data = help_data;

  this->AsyncQueueWork(
      index, loop, work_req,
      [](uv_work_t *work_req) {
        get_obj_helper *help_data = (get_obj_helper *)work_req->data;
        work_data *data = MemSaver::Default()->work_datas_[help_data->idx];
        DicTable<object_t>::Node *node =
            data->db->Get(rockin::make_object(help_data->key));
        if (node != nullptr) help_data->obj = node->data;
      },
      [](uv_work_t *work_req, int status) {
        get_obj_helper *help_data = (get_obj_helper *)work_req->data;
        help_data->callback(help_data->obj);
        delete help_data;
        free(work_req);
      });
}

// get objs from memsaver
void MemSaver::GetObj(uv_loop_t *loop, BufPtrs keys,
                      std::function<void(ObjPtrs)> callback) {
  //
}

struct insert_obj_helper {
  int idx;
  ObjPtr obj;
  std::function<void(ObjPtr)> callback;
};
// insert obj to memsaver
void MemSaver::InsertObj(uv_loop_t *loop, ObjPtr obj,
                         std::function<void(ObjPtr)> callback) {
  if (thread_num_ == 0) return;
  int index = rockin::Hash(obj->key->data, obj->key->len) % thread_num_;

  insert_obj_helper *help_data = new insert_obj_helper();
  help_data->idx = index;
  help_data->obj = obj;
  help_data->callback = callback;

  uv_work_t *work_req = (uv_work_t *)malloc(sizeof(uv_work_t));
  work_req->data = help_data;

  this->AsyncQueueWork(
      index, loop, work_req,
      [](uv_work_t *work_req) {
        insert_obj_helper *help_data = (insert_obj_helper *)work_req->data;
        work_data *data = MemSaver::Default()->work_datas_[help_data->idx];

        DicTable<object_t>::Node *node = new DicTable<object_t>::Node();
        node->data = help_data->obj;
        node->next = nullptr;
        data->db->Insert(node);
      },
      [](uv_work_t *work_req, int status) {
        insert_obj_helper *help_data = (insert_obj_helper *)work_req->data;
        help_data->callback(help_data->obj);
        delete help_data;
        free(work_req);
      });
}

// insert obj to memsaver
void MemSaver::InsertObj(uv_loop_t *loop, ObjPtrs obj,
                         std::function<void(ObjPtrs)> callback) {
  //
}

}  // namespace rockin
