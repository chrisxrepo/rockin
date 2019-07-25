#pragma once
#include <functional>
#include <memory>
#include <vector>
#include "async.h"
#include "mem_alloc.h"

struct uv__work;

namespace rocksdb {
class Cache;
}

namespace rockin {
struct DiskDB;

struct WriteAsyncQueue {
  QUEUE queue;
  uv_cond_t cond;
  uv_mutex_t mutex;
  std::vector<QUEUE *> queues;
  uint64_t snum;

  WriteAsyncQueue() : snum(0) {
    // init mutex
    int retcode = uv_mutex_init(&mutex);
    LOG_IF(FATAL, retcode) << "uv_mutex_init errer:" << GetUvError(retcode);

    // init cond
    retcode = uv_cond_init(&cond);
    LOG_IF(FATAL, retcode) << "uv_cond_init errer:" << GetUvError(retcode);

    // init queue
    QUEUE_INIT(&queue);
  }
};

class DiskSaver {
 public:
  static DiskSaver *Default();
  static void DestoryDefault();

  DiskSaver();
  ~DiskSaver();

  bool Init(int partition_num, const std::string &path);

  // get meta
  std::string GetMeta(BufPtr mkey, bool &exist);

  // get array value
  std::vector<std::string> GetValues(BufPtr mkey, std::vector<BufPtr> keys,
                                     std::vector<bool> &exists);

  bool Set(BufPtr mkey, BufPtr meta);
  bool Set(BufPtr mkey, KVPairS kvs);
  bool Set(BufPtr mkey, BufPtr meta, KVPairS kvs);

  void Compact();

 private:
  DiskDB *GetDB(BufPtr key);
  void WriteBatch(int idx, const std::vector<uv__work *> &works);

 private:
  std::string path_;
  size_t partition_num_;

  std::vector<DiskDB *> dbs_;
  std::shared_ptr<rocksdb::Cache> meta_cache_;
  std::shared_ptr<rocksdb::Cache> data_cache_;
};

}  // namespace rockin