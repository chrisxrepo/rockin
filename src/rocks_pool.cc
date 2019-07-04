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

void RocksPool::DestoryRocksPool() {
  if (g_pool != nullptr) {
    g_pool = nullptr;
  }
}

RocksPool::RocksPool() {
  rocksdb::Env *env = rocksdb::Env::Default();
  env->SetBackgroundThreads(2, rocksdb::Env::LOW);
  env->SetBackgroundThreads(1, rocksdb::Env::HIGH);
}

RocksPool::~RocksPool() {
  LOG(INFO) << "destroy rocks pool...";
  dbs_.clear();
}

void RocksPool::Init(int partition_num, const std::string &path) {
  for (int i = 0; i < partition_num; i++) {
    std::string name = Format("partition_%05d", i);
    std::string dbpath = path + "/" + name;
    auto db = std::make_shared<RocksDB>(i, name, dbpath);
    dbs_.push_back(db);
  }
}

void RocksPool::Init(int partition_num, std::vector<int> partitions,
                     const std::string &path) {
  for (int i = 0; i < partition_num; i++) {
    dbs_.push_back(nullptr);
  }

  for (auto iter = partitions.begin(); iter != partitions.end(); ++iter) {
    int partition_id = *iter;
    if (partition_id >= partition_num) continue;

    std::string name = Format("partition_%05d", partition_id);
    std::string dbpath = path + "/" + name;
    auto db = std::make_shared<RocksDB>(partition_id, name, dbpath);
    dbs_[partition_id] = db;
  }
}

}  // namespace rockin