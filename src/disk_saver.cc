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
  MemPtr key;
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

DiskSaver::DiskSaver() : write_queue_(0xF00000) {
  rocksdb::Env *env = rocksdb::Env::Default();
  env->SetBackgroundThreads(2, rocksdb::Env::LOW);
  env->SetBackgroundThreads(1, rocksdb::Env::HIGH);

  uv_loop_init(&write_loop_);
  uv_thread_create(&write_thread_,
                   [](void *arg) {
                     DiskSaver *ds = (DiskSaver *)arg;
                     ds->RunLoop();
                   },
                   this);
}

DiskSaver::~DiskSaver() {
  LOG(INFO) << "destroy rocks pool...";
  partitions_.clear();
}

void DiskSaver::InitNoCrteate(int partition_num, const std::string &path) {
  path_ = path;
  for (int i = 0; i < partition_num; i++) {
    partitions_.push_back(nullptr);
  }
}

void DiskSaver::InitAndCreate(int partition_num, const std::string &path) {
  path_ = path;
  for (int i = 0; i < partition_num; i++) {
    std::string name = Format("partition_%05d", i);
    std::string dbpath = path + "/" + name;
    auto db = std::make_shared<DiskDB>(i, name, dbpath);
    partitions_.push_back(db);
  }
}

void DiskSaver::InitAndCreate(int partition_num, std::vector<int> partitions,
                              const std::string &path) {
  path_ = path;
  for (int i = 0; i < partition_num; i++) {
    partitions_.push_back(nullptr);
  }

  for (auto iter = partitions.begin(); iter != partitions.end(); ++iter) {
    int partition_id = *iter;
    if (partition_id >= partition_num) continue;

    std::string name = Format("partition_%05d", partition_id);
    std::string dbpath = path + "/" + name;
    auto db = std::make_shared<DiskDB>(partition_id, name, dbpath);
    partitions_[partition_id] = db;
  }
}

void DiskSaver::RunLoop() {
  write_async_.data = this;
  uv_async_init(&write_loop_, &write_async_, [](uv_async_t *handle) {
    DiskSaver *ds = (DiskSaver *)handle->data;
    ds->WriteDiskData();
  });

  while (true) {
    uv_run(&write_loop_, UV_RUN_DEFAULT);
  }
}

static int partition_index(MemPtr key, size_t partition_num) {
  return HashCode(key->data, key->len) % partition_num;
}

void DiskSaver::WriteDiskData() {
  int partition_num = partitions_.size();
  while (true) {
    std::map<int, std::vector<DiskWrite>> write_map;
    std::vector<std::shared_ptr<void>> retains;
    while (true) {
      _WriteData *wd = write_queue_.Pop();
      if (wd == nullptr) break;

      retains.push_back(wd->retain);
      int partition = partition_index(wd->key, partition_num);
      for (auto iter = wd->writes.begin(); iter != wd->writes.end(); ++iter) {
        write_map[partition].push_back(*iter);
      }
      delete wd;
    }

    if (write_map.size() == 0) break;

    for (auto iter = write_map.begin(); iter != write_map.end(); ++iter) {
      // handle write
      auto db = partitions_[iter->first];
      if (db != nullptr) {
        db->WriteBatch(iter->second);
      } else {
        LOG(ERROR) << "Partition" << iter->first << " is not in this instance.";
      }
    }
  }
}

std::shared_ptr<DiskDB> DiskSaver::GetDB(MemPtr key) {
  int index = partition_index(key, partitions_.size());
  return partitions_[index];
}

bool DiskSaver::WriteMeta(int dbindex, MemPtr key, KVPairS mates,
                          std::shared_ptr<void> retain) {
  return WriteAll(dbindex, key, mates, KVPairS(), retain);
}

bool DiskSaver::WriteData(int dbindex, MemPtr key, KVPairS datas,
                          std::shared_ptr<void> retain) {
  return WriteAll(dbindex, key, KVPairS(), datas, retain);
}

bool DiskSaver::WriteAll(int dbindex, MemPtr key, KVPairS mates, KVPairS datas,
                         std::shared_ptr<void> retain) {
  _WriteData *wd = new _WriteData;
  wd->key = key;
  wd->retain = retain;

  for (auto iter = mates.begin(); iter != mates.end(); ++iter) {
    wd->writes.push_back(
        DiskWrite(dbindex, Write_Meta, iter->first, iter->second));
  }
  for (auto iter = datas.begin(); iter != datas.end(); ++iter) {
    wd->writes.push_back(
        DiskWrite(dbindex, Write_Data, iter->first, iter->second));
  }

  while (!write_queue_.Push(wd))
    ;
  uv_async_send(&write_async_);
  return true;
}

}  // namespace rockin