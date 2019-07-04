#pragma once
#include <memory>
#include <vector>
#include "rocks_db.h"

namespace rockin {
class RocksPool {
 public:
  static RocksPool *GetInstance();
  static void DestoryRocksPool();

  RocksPool();
  ~RocksPool();

  void Init(int partition_num, const std::string &path);
  void Init(int partition_num, std::vector<int> dbs, const std::string &path);

 private:
  std::vector<std::shared_ptr<RocksDB>> dbs_;
  static RocksPool *g_pool;
};

}  // namespace rockin