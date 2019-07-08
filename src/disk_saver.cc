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

DiskSaver::DiskSaver() {
  rocksdb::Env *env = rocksdb::Env::Default();
  env->SetBackgroundThreads(2, rocksdb::Env::LOW);
  env->SetBackgroundThreads(1, rocksdb::Env::HIGH);
}

DiskSaver::~DiskSaver() {
  LOG(INFO) << "destroy rocks pool...";
  dbs_.clear();
}

void DiskSaver::InitNoCrteate(int partition_num, const std::string &path) {
  path_ = path;
  for (int i = 0; i < partition_num; i++) {
    dbs_.push_back(nullptr);
  }
}

void DiskSaver::InitAndCreate(int partition_num, const std::string &path) {
  path_ = path;
  for (int i = 0; i < partition_num; i++) {
    std::string name = Format("partition_%05d", i);
    std::string dbpath = path + "/" + name;
    auto db = std::make_shared<DiskDB>(i, name, dbpath);
    dbs_.push_back(db);
  }
}

void DiskSaver::InitAndCreate(int partition_num, std::vector<int> partitions,
                              const std::string &path) {
  path_ = path;
  for (int i = 0; i < partition_num; i++) {
    dbs_.push_back(nullptr);
  }

  for (auto iter = partitions.begin(); iter != partitions.end(); ++iter) {
    int partition_id = *iter;
    if (partition_id >= partition_num) continue;

    std::string name = Format("partition_%05d", partition_id);
    std::string dbpath = path + "/" + name;
    auto db = std::make_shared<DiskDB>(partition_id, name, dbpath);
    dbs_[partition_id] = db;
  }
}

std::shared_ptr<DiskDB> DiskSaver::GetDB(std::shared_ptr<buffer_t> key) {
  int index = HashCode(key->data, key->len) % dbs_.size();
  return dbs_[index];
}

}  // namespace rockin