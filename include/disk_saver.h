#pragma once
#include <uv.h>
#include <memory>
#include <vector>
#include "async.h"
#include "disk_db.h"
#include "mem_alloc.h"

namespace rockin {
class DiskSaver : public Async {
 public:
  static DiskSaver *Default();
  static void DestoryDefault();

  DiskSaver();
  ~DiskSaver();

  bool Init(int thread_num, int partition_num, const std::string &path);

  void GetData(BufPtr key);

  // get diskdb
  std::shared_ptr<DiskDB> GetDB(BufPtr key);

  void Compact();

 private:
  void AsyncWork(int idx) override;
  void PostWork(int idx, QUEUE *q) override;

 private:
  std::string path_;
  size_t partition_num_;
  size_t thread_num_;
  std::vector<std::shared_ptr<DiskDB>> partitions_;

  uv_mutex_t mutex_;
  uv_cond_t cond_;
  QUEUE queue_;
};

}  // namespace rockin