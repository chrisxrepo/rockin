#pragma once
#include <memory>
#include <vector>
#include "rocks_db.h"

namespace rockin {
class RocksPool {
 public:
  static RocksPool *GetInstance();

  void Init(int partnum, const std::string &path);

 private:
  std::vector<std::shared_ptr<RocksDB>> dbs_;
  static RocksPool *g_pool;
};

}  // namespace rockin