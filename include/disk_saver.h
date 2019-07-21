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
struct DiskRocksdb;

class DiskSaver : public Async {
 public:
  static DiskSaver *Default();
  static void DestoryDefault();

  DiskSaver();
  ~DiskSaver();

  bool Init(int read_thread_num, int write_thread_num, int partition_num,
            const std::string &path);

  // get meta
  typedef std::function<void(bool, BufPtr, const std::string &)> MetaCB;
  void GetMeta(uv_loop_t *loop, BufPtr mkey, MetaCB cb);

  // get meta and array value
  typedef std::vector<bool> Exists;
  typedef std::vector<std::string> Results;
  typedef std::function<void(BufPtr, const Exists &, const Results &)>
      ArrayValueCB;
  void GetValues(uv_loop_t *loop, BufPtr mkey, std::vector<BufPtr> keys,
                 ArrayValueCB cb);

  typedef std::function<void(bool)> SetCB;
  void Set(uv_loop_t *loop, BufPtr mkey, BufPtr meta, SetCB cb);
  void Set(uv_loop_t *loop, BufPtr mkey, BufPtrs keys, BufPtrs values,
           SetCB cb);
  void Set(uv_loop_t *loop, BufPtr mkey, BufPtr meta, BufPtrs keys,
           BufPtrs values, SetCB cb);

  void Compact();

 private:
  void AsyncWork(int idx) override;
  void ReadAsyncWork(int idx);
  void WriteAsyncWork(int idx);
  void PostWork(int idx, QUEUE *q) override;
  DiskRocksdb *GetDiskRocksdb(BufPtr key);
  void WriteBatch(int idx, const std::vector<uv__work *> &works);

 private:
  std::string path_;
  size_t partition_num_;
  size_t read_thread_num_;
  size_t write_thread_num_;

  std::vector<DiskRocksdb *> dbs_;
  std::shared_ptr<rocksdb::Cache> meta_cache_;
  std::shared_ptr<rocksdb::Cache> data_cache_;

  uv_mutex_t read_mutex_;
  uv_cond_t read_cond_;
  QUEUE read_queue_;
  std::vector<AsyncQueue *> writes_;
};

}  // namespace rockin