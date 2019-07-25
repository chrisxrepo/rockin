#include "disk_saver.h"
#include <glog/logging.h>
#include <rocksdb/db.h>
#include <rocksdb/table.h>
#include <mutex>

#include "compact_filter.h"
#include "rocksdb/filter_policy.h"
#include "siphash.h"
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

struct DiskDB {
  rocksdb::DB *db;
  rocksdb::ColumnFamilyHandle *mt_handle;
  rocksdb::ColumnFamilyHandle *db_handle;
  std::string partition_name;
  int partition_id;
};

DiskSaver::DiskSaver() : partition_num_(0) {}

DiskSaver::~DiskSaver() {
  LOG(INFO) << "destroy rocks pool...";
  // partitions_.clear();
}

bool DiskSaver::Init(int partition_num, const std::string &path) {
  path_ = path;
  partition_num_ = partition_num;

  rocksdb::Env *env = rocksdb::Env::Default();
  env->SetBackgroundThreads(2, rocksdb::Env::LOW);
  env->SetBackgroundThreads(1, rocksdb::Env::HIGH);

  meta_cache_ = rocksdb::NewLRUCache(1 << 30);
  data_cache_ = rocksdb::NewLRUCache(128 << 20);

  for (int i = 0; i < partition_num; i++) {
    DiskDB *db = new DiskDB();
    std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
    std::string partition_name = Format("partition_%05d", i);
    std::string dbpath = path + "/" + partition_name;

    // meta
    rocksdb::ColumnFamilyOptions mt_cf_ops;
    rocksdb::BlockBasedTableOptions mt_table_ops;
    mt_table_ops.block_size = 4096;  // 4k
    mt_table_ops.partition_filters = true;
    mt_table_ops.index_type =
        rocksdb::BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
    mt_table_ops.block_cache = meta_cache_;
    mt_table_ops.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, true));
    mt_cf_ops.table_factory.reset(
        rocksdb::NewBlockBasedTableFactory(mt_table_ops));
    mt_cf_ops.compaction_filter_factory =
        std::make_shared<MetaCompactFilterFactory>(partition_name + ":mt");
    column_families.push_back(rocksdb::ColumnFamilyDescriptor("mt", mt_cf_ops));

    // data
    rocksdb::ColumnFamilyOptions db_cf_ops;
    rocksdb::BlockBasedTableOptions db_table_ops;
    db_table_ops.block_size = 4096;  // 4k
    db_table_ops.block_cache = data_cache_;
    db_table_ops.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, true));
    db_cf_ops.table_factory.reset(
        rocksdb::NewBlockBasedTableFactory(db_table_ops));
    db_cf_ops.compaction_filter_factory =
        std::make_shared<DataCompactFilterFactory>(partition_name + ":data",
                                                   &db->db, &db->mt_handle);
    column_families.push_back(
        rocksdb::ColumnFamilyDescriptor("data", db_cf_ops));

    // default
    column_families.push_back(rocksdb::ColumnFamilyDescriptor(
        rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions()));

    rocksdb::Options ops;
    ops.env = rocksdb::Env::Default();
    ops.create_if_missing = true;
    ops.create_missing_column_families = true;
    ops.write_buffer_size = 256 << 20;        // 256M
    ops.target_file_size_base = 20 << 20;     // 20M
    ops.max_background_flushes = 1;           // flush后台线程数量
    ops.max_background_compactions = 2;       // compact线程数量
    ops.max_bytes_for_level_multiplier = 10;  // 每层SST文件大小因子
    ops.optimize_filters_for_hits = false;  // True:最大层STT文件没有filter
    ops.level_compaction_dynamic_level_bytes = false;
    ops.max_open_files = 5000;

    std::vector<rocksdb::ColumnFamilyHandle *> handles;

    // ops.write_buffer_manager
    auto status =
        rocksdb::DB::Open(ops, dbpath, column_families, &handles, &db->db);
    LOG_IF(FATAL, !status.ok())
        << "rocksdb::DB::Open status:" << status.getState();
    LOG_IF(FATAL, column_families.size() != 3)
        << "column family size:" << column_families.size();

    db->partition_id = i;
    db->partition_name = partition_name;
    db->mt_handle = handles[0];
    db->db_handle = handles[1];
    LOG(INFO) << "open rocksdb:" << partition_name;
    dbs_.push_back(db);
  }

  return true;
}

DiskDB *DiskSaver::GetDB(BufPtr key) {
  if (DiskSaver::Default()->partition_num_ == 1) return dbs_[0];
  int index = rockin::SimpleHash(key->data, key->len) % partition_num_;
  return dbs_[index];
}

static bool GetFromRocksdb(rocksdb::DB *db, rocksdb::ColumnFamilyHandle *handle,
                           std::vector<BufPtr> &keys, std::vector<bool> &exists,
                           std::vector<std::string> &value) {
  exists.clear();
  std::vector<rocksdb::Slice> key_slices;
  std::vector<rocksdb::ColumnFamilyHandle *> handles;
  for (size_t i = 0; i < keys.size(); i++) {
    handles.push_back(handle);
    key_slices.push_back(rocksdb::Slice(keys[i]->data, keys[i]->len));
  }

  auto statuss =
      db->MultiGet(rocksdb::ReadOptions(), handles, key_slices, &value);
  for (size_t i = 0; i < statuss.size(); i++) {
    if (statuss[i].ok())
      exists.push_back(true);
    else if (statuss[i].IsNotFound())
      exists.push_back(false);
    else {
      LOG(ERROR) << "rocksdb MuiltGet:" << statuss[i].ToString();
      return false;
    }
  }

  return true;
}

std::string DiskSaver::GetMeta(BufPtr mkey, bool &exist) {
  DiskDB *diskDB = this->GetDB(mkey);

  std::string value;
  auto status = diskDB->db->Get(rocksdb::ReadOptions(), diskDB->mt_handle,
                                rocksdb::Slice(mkey->data, mkey->len), &value);
  if (status.ok()) {
    exist = true;
    return std::move(value);
  }

  if (!status.IsNotFound()) LOG(ERROR) << "rocksdb Get:" << status.ToString();
  return "";
}

std::vector<std::string> DiskSaver::GetValues(BufPtr mkey, BufPtrs keys,
                                              std::vector<bool> &exists) {
  DiskDB *diskDB = this->GetDB(mkey);

  exists.clear();
  std::vector<rocksdb::Slice> key_slices;
  std::vector<rocksdb::ColumnFamilyHandle *> handles;
  for (size_t i = 0; i < keys.size(); i++) {
    handles.push_back(diskDB->db_handle);
    key_slices.push_back(rocksdb::Slice(keys[i]->data, keys[i]->len));
  }

  std::vector<std::string> values;
  auto statuss = diskDB->db->MultiGet(rocksdb::ReadOptions(), handles,
                                      key_slices, &values);
  for (size_t i = 0; i < statuss.size(); i++) {
    if (statuss[i].ok())
      exists.push_back(true);
    else {
      exists.push_back(false);
      if (!statuss[i].IsNotFound())
        LOG(ERROR) << "rocksdb MuiltGet:" << statuss[i].ToString();
    }
  }

  return std::move(values);
}

bool DiskSaver::Set(BufPtr mkey, BufPtr meta) {
  DiskDB *diskDB = this->GetDB(mkey);

  auto status = diskDB->db->Put(rocksdb::WriteOptions(), diskDB->mt_handle,
                                rocksdb::Slice(mkey->data, mkey->len),
                                rocksdb::Slice(meta->data, meta->len));

  if (!status.ok()) LOG(ERROR) << "rocksdb Put:" << status.ToString();
  return status.ok();
}

bool DiskSaver::Set(BufPtr mkey, KVPairS kvs) {
  DiskDB *diskDB = this->GetDB(mkey);

  rocksdb::WriteBatch batch;
  for (auto iter = kvs.begin(); iter != kvs.end(); ++iter) {
    auto status = batch.Put(
        diskDB->db_handle, rocksdb::Slice(iter->first->data, iter->first->len),
        rocksdb::Slice(iter->second->data, iter->second->len));
    if (!status.ok()) {
      LOG(ERROR) << "rocksdb WriteBatch.Put:" << status.ToString();
      return false;
    }
  }

  auto status = diskDB->db->Write(rocksdb::WriteOptions(), &batch);
  if (!status.ok()) LOG(ERROR) << "rocksdb Write:" << status.ToString();
  return status.ok();
}

bool DiskSaver::Set(BufPtr mkey, BufPtr meta, KVPairS kvs) {
  DiskDB *diskDB = this->GetDB(mkey);

  rocksdb::WriteBatch batch;
  auto status =
      batch.Put(diskDB->mt_handle, rocksdb::Slice(mkey->data, mkey->len),
                rocksdb::Slice(meta->data, meta->len));
  if (!status.ok()) {
    LOG(ERROR) << "rocksdb WriteBatch.Put:" << status.ToString();
    return false;
  }

  for (auto iter = kvs.begin(); iter != kvs.end(); ++iter) {
    auto status = batch.Put(
        diskDB->db_handle, rocksdb::Slice(iter->first->data, iter->first->len),
        rocksdb::Slice(iter->second->data, iter->second->len));
    if (!status.ok()) {
      LOG(ERROR) << "rocksdb WriteBatch.Put:" << status.ToString();
      return false;
    }
  }

  status = diskDB->db->Write(rocksdb::WriteOptions(), &batch);
  if (!status.ok()) LOG(ERROR) << "rocksdb Write:" << status.ToString();
  return status.ok();
}

void DiskSaver::Compact() {
  for (size_t i = 0; i < dbs_.size(); i++) {
    LOG(INFO) << "Start to compct rocksdb:" << dbs_[i]->partition_name;

    auto status = dbs_[i]->db->CompactRange(
        rocksdb::CompactRangeOptions(), dbs_[i]->mt_handle, nullptr, nullptr);
    if (!status.ok()) {
      LOG(ERROR) << "Compact partition:" << dbs_[i]->partition_name
                 << ", metadata error:" << status.ToString();
    }

    status = dbs_[i]->db->CompactRange(rocksdb::CompactRangeOptions(),
                                       dbs_[i]->db_handle, nullptr, nullptr);
    if (!status.ok()) {
      LOG(ERROR) << "Compact partition:" << dbs_[i]->partition_name
                 << ", dbdata error:" << status.ToString();
    }
  }
}

}  // namespace rockin