#include "rocks_pool.h"
#include <glog/logging.h>
#include <rocksdb/db.h>
#include "utils.h"

namespace rockin {
RocksPool *RocksPool::g_pool = nullptr;

RocksPool *RocksPool::GetInstance() {
  if (g_pool == nullptr) {
    g_pool = new RocksPool();
  }
  return g_pool;
}

void RocksPool::Init(int partnum, const std::string &path) {
  rocksdb::Env *env = rocksdb::Env::Default();
  env->SetBackgroundThreads(2, rocksdb::Env::LOW);
  env->SetBackgroundThreads(1, rocksdb::Env::HIGH);
  rocksdb::Options options;
  options.env = env;
  options.max_background_compactions = 2;
  options.max_background_flushes = 1;
  options.compaction_filter_factory = nullptr;

  for (int i = 0; i < partnum; i++) {
    std::string dbpath = path + "/" + Format("db_%05d", i + 0);
    auto db = std::make_shared<RocksDB>(dbpath, options);
    dbs_.push_back(db);
  }
}

}  // namespace rockin