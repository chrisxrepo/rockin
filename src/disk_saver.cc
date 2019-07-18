#include "disk_saver.h"
#include <glog/logging.h>
#include <rocksdb/db.h>
#include <mutex>
#include "utils.h"

namespace rockin {

namespace {
std::once_flag disk_saver_once_flag;
DiskSaver *g_disk_saver;
};  // namespace

class _WriteData {
 public:
  BufPtr key;
  std::vector<DiskWrite> writes;
  std::shared_ptr<void> retain;
};

DiskSaver *DiskSaver::Default() {
  std::call_once(disk_saver_once_flag,
                 []() { g_disk_saver = new DiskSaver(); });
  return g_disk_saver;
}

void DiskSaver::DestoryDefault() {
  if (g_disk_saver == nullptr) {
    delete g_disk_saver;
    g_disk_saver = nullptr;
  }
}

DiskSaver::DiskSaver() : partition_num_(0), thread_num_(0) {
  int retcode = uv_mutex_init(&mutex_);
  LOG_IF(FATAL, retcode) << "uv_mutex_init errer:" << GetUvError(retcode);

  retcode = uv_cond_init(&cond_);
  LOG_IF(FATAL, retcode) << "uv_cond_init errer:" << GetUvError(retcode);

  QUEUE_INIT(&queue_);

  rocksdb::Env *env = rocksdb::Env::Default();
  env->SetBackgroundThreads(2, rocksdb::Env::LOW);
  env->SetBackgroundThreads(1, rocksdb::Env::HIGH);
}

DiskSaver::~DiskSaver() {
  LOG(INFO) << "destroy rocks pool...";
  partitions_.clear();
}

bool DiskSaver::Init(int thread_num, int partition_num,
                     const std::string &path) {
  path_ = path;
  thread_num_ = thread_num;
  partition_num_ = partition_num;
  for (int i = 0; i < partition_num; i++) {
    std::string name = Format("partition_%05d", i);
    std::string dbpath = path + "/" + name;
    auto db = std::make_shared<DiskDB>(i, name, dbpath);
    partitions_.push_back(db);
  }

  return this->InitAsync(thread_num);
}

void DiskSaver::AsyncWork(int idx) {
  while (true) {
    uv_mutex_lock(&mutex_);
    while (QUEUE_EMPTY(&queue_)) {
      uv_cond_wait(&cond_, &mutex_);
    }

    QUEUE *q = QUEUE_HEAD(&queue_);
    QUEUE_REMOVE(q);
    uv_mutex_unlock(&mutex_);

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

void DiskSaver::PostWork(int idx, QUEUE *q) {
  uv_mutex_lock(&mutex_);
  QUEUE_INSERT_TAIL(&queue_, q);
  uv_cond_signal(&cond_);
  uv_mutex_unlock(&mutex_);
}

std::shared_ptr<DiskDB> DiskSaver::GetDB(BufPtr key) {
  if (partition_num_ == 1) return partitions_[0];
  int index = HashCode(key->data, key->len) % partition_num_;
  return partitions_[index];
}

void DiskSaver::Compact() {
  for (size_t i = 0; i < partitions_.size(); i++) {
    partitions_[i]->Compact();
  }
}

}  // namespace rockin