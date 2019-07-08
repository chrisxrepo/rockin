#include "disk_db.h"
#include <assert.h>
#include <glog/logging.h>
#include <rocksdb/table.h>
#include "compact_filter.h"
#include "rocksdb/filter_policy.h"
#include "type_common.h"
#include "utils.h"

namespace rockin {
std::shared_ptr<rocksdb::Cache> DiskDB::g_meta_block_cache =
    rocksdb::NewLRUCache(1 << 30);
std::shared_ptr<rocksdb::Cache> DiskDB::g_data_block_cache =
    rocksdb::NewLRUCache(128 << 20);

DiskDB::DiskDB(int partion_id, const std::string partition_name,
               const std::string &path)
    : partition_id_(partion_id), partition_name_(partition_name), path_(path) {
  std::vector<rocksdb::ColumnFamilyDescriptor> column_families;

  // meta
  for (int i = 0; i < DBNum; i++) {
    rocksdb::BlockBasedTableOptions table_ops;
    table_ops.block_size = 4096;  // 4k
    table_ops.cache_index_and_filter_blocks = true;
    table_ops.block_cache = g_meta_block_cache;
    table_ops.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, true));

    std::string cf_name = Format("mt_%05d", i);
    std::string filter_name = partition_name_ + ":" + cf_name;
    rocksdb::ColumnFamilyOptions cf_ops;
    cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_ops));
    cf_ops.compaction_filter_factory =
        std::make_shared<MetaCompactFilterFactory>(filter_name);
    column_families.push_back(rocksdb::ColumnFamilyDescriptor(cf_name, cf_ops));
  }

  // data
  for (int i = 0; i < DBNum; i++) {
    rocksdb::BlockBasedTableOptions table_ops;
    table_ops.block_size = 4096;  // 4k
    table_ops.cache_index_and_filter_blocks = true;
    table_ops.block_cache = g_data_block_cache;
    table_ops.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, true));

    std::string cf_name = Format("db_%05d", i);
    std::string filter_name = partition_name_ + ":" + cf_name;
    rocksdb::ColumnFamilyOptions cf_ops;
    cf_ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_ops));
    cf_ops.compaction_filter_factory =
        std::make_shared<DataCompactFilterFactory>(filter_name, &mt_handles_,
                                                   i);
    column_families.push_back(rocksdb::ColumnFamilyDescriptor(cf_name, cf_ops));
  }

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
  auto status = rocksdb::DB::Open(ops, path, column_families, &handles, &db_);
  LOG_IF(FATAL, !status.ok())
      << "rocksdb::DB::Open status:" << status.getState();
  LOG_IF(FATAL, column_families.size() != DBNum * 2 + 1)
      << "column family size:" << column_families.size();

  for (int i = 0; i < DBNum * 2; i++) {
    if (i < DBNum)
      mt_handles_.push_back(handles[i]);
    else
      db_handles_.push_back(handles[i]);
  }

  LOG(INFO) << "open rocksdb:" << partition_name_
            << ", data size:" << GetSizeString(GetDirectorySize(path))
            << ", column family size:" << column_families.size();
}

DiskDB::~DiskDB() {
  if (db_) {
    for (int i = 0; i < DBNum; i++) {
      db_->DestroyColumnFamilyHandle(mt_handles_[i]);
      db_->DestroyColumnFamilyHandle(db_handles_[i]);
    }

    delete db_;
  }
}

bool DiskDB::GetMeta(int db, const std::string &key, std::string *value) {
  if (db < 0 || db >= mt_handles_.size()) {
    return false;
  }

  return Get(mt_handles_[db], key, value);
}

bool DiskDB::GetData(int db, const std::string &key, std::string *value) {
  if (db < 0 || db >= db_handles_.size()) {
    return false;
  }

  return Get(db_handles_[db], key, value);
}

bool DiskDB::SetMeta(int db, const std::string &key, const std::string &value) {
  if (db < 0 || db >= mt_handles_.size()) {
    return false;
  }

  return Set(mt_handles_[db], key, value);
}

bool DiskDB::SetData(int db, const std::string &key, const std::string &value) {
  if (db < 0 || db >= db_handles_.size()) {
    return false;
  }

  return Set(db_handles_[db], key, value);
}

bool DiskDB::SetAll(
    int db, const std::vector<std::pair<std::string, std::string>> &metas,
    const std::vector<std::pair<std::string, std::string>> &datas) {
  if (db < 0 || db >= db_handles_.size()) {
    return false;
  }

  rocksdb::WriteBatch batch;
  for (auto iter = metas.begin(); iter != metas.end(); ++iter) {
    auto status = batch.Put(mt_handles_[db], iter->first, iter->second);
    if (!status.ok()) {
      LOG(ERROR) << "rocksdb WriteBatch.Put:" << status.ToString();
      return false;
    }
  }
  for (auto iter = datas.begin(); iter != datas.end(); ++iter) {
    auto status = batch.Put(mt_handles_[db], iter->first, iter->second);
    if (!status.ok()) {
      LOG(ERROR) << "rocksdb WriteBatch.Put:" << status.ToString();
      return false;
    }
  }

  auto status = db_->Write(rocksdb::WriteOptions(), &batch);
  if (!status.ok()) {
    LOG(ERROR) << "rocksdb Write:" << status.ToString();
    return false;
  }

  return true;
}

bool DiskDB::Get(rocksdb::ColumnFamilyHandle *cf, const std::string &key,
                 std::string *value) {
  auto status = db_->Get(rocksdb::ReadOptions(), cf,
                         rocksdb::Slice(key.c_str(), key.length()), value);

  if (status.IsNotFound()) {
    return false;
  }

  if (!status.ok()) {
    LOG(ERROR) << "rocksdb Get:" << status.ToString();
    return false;
  }

  return true;
}

bool DiskDB::Set(rocksdb::ColumnFamilyHandle *cf, const std::string &key,
                 const std::string &value) {
  auto status = db_->Put(rocksdb::WriteOptions(), cf,
                         rocksdb::Slice(key.c_str(), key.length()),
                         rocksdb::Slice(value.c_str(), value.length()));

  if (!status.ok()) {
    LOG(ERROR) << "rocksdb Put:" << status.ToString();
    return false;
  }

  return true;
}

}  // namespace rockin