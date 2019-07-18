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
  rocksdb::ColumnFamilyOptions mt_cf_ops;
  rocksdb::BlockBasedTableOptions mt_table_ops;
  mt_table_ops.block_size = 4096;  // 4k
  mt_table_ops.cache_index_and_filter_blocks = true;
  mt_table_ops.block_cache = g_meta_block_cache;
  mt_table_ops.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, true));
  mt_cf_ops.table_factory.reset(
      rocksdb::NewBlockBasedTableFactory(mt_table_ops));
  mt_cf_ops.compaction_filter_factory =
      std::make_shared<MetaCompactFilterFactory>(partition_name_ + ":mt");
  column_families.push_back(rocksdb::ColumnFamilyDescriptor("mt", mt_cf_ops));

  // data
  rocksdb::ColumnFamilyOptions db_cf_ops;
  rocksdb::BlockBasedTableOptions db_table_ops;
  db_table_ops.block_size = 4096;  // 4k
  db_table_ops.cache_index_and_filter_blocks = true;
  db_table_ops.block_cache = g_data_block_cache;
  db_table_ops.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, true));
  db_cf_ops.table_factory.reset(
      rocksdb::NewBlockBasedTableFactory(db_table_ops));
  db_cf_ops.compaction_filter_factory =
      std::make_shared<DataCompactFilterFactory>(partition_name_ + ":data",
                                                 &db_, &mt_handle_);
  column_families.push_back(rocksdb::ColumnFamilyDescriptor("data", db_cf_ops));

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
  auto status = rocksdb::DB::Open(ops, path, column_families, &handles, &db_);
  LOG_IF(FATAL, !status.ok())
      << "rocksdb::DB::Open status:" << status.getState();
  LOG_IF(FATAL, column_families.size() != 3)
      << "column family size:" << column_families.size();

  mt_handle_ = handles[0];
  db_handle_ = handles[1];

  LOG(INFO) << "open rocksdb:" << partition_name_;
}

DiskDB::~DiskDB() {
  if (db_) {
    delete db_;
  }
}

bool DiskDB::GetMeta(BufPtr key, std::string *value) {
  auto status = db_->Get(read_options_, mt_handle_,
                         rocksdb::Slice(key->data, key->len), value);

  if (status.IsNotFound()) {
    return false;
  }

  if (!status.ok()) {
    LOG(ERROR) << "GetMeta Get[" << std::string(key->data, key->len)
               << "] error:" << status.ToString();
    return false;
  }

  return true;
}

bool DiskDB::GetData(BufPtr key, std::string *value) {
  auto status = db_->Get(read_options_, db_handle_,
                         rocksdb::Slice(key->data, key->len), value);

  if (status.IsNotFound()) {
    return false;
  }

  if (!status.ok()) {
    LOG(ERROR) << "GetData Get[" << std::string(key->data, key->len)
               << "] error:" << status.ToString();
    return false;
  }

  return true;
}

std::vector<bool> DiskDB::GetDatas(std::vector<BufPtr> &keys,
                                   std::vector<std::string> *values) {
  std::vector<rocksdb::Slice> slices;
  std::vector<rocksdb::ColumnFamilyHandle *> cfs;
  for (size_t i = 0; i < keys.size(); i++) {
    cfs.push_back(db_handle_);
    slices.push_back(rocksdb::Slice(keys[i]->data, keys[i]->len));
  }

  auto statuss = db_->MultiGet(read_options_, cfs, slices, values);

  std::vector<bool> results;
  for (size_t i = 0; i < statuss.size(); i++) {
    if (statuss[i].ok()) {
      results.push_back(true);
      continue;
    }

    if (!statuss[i].IsNotFound()) {
      LOG(ERROR) << "GetDatas MGet[" << std::string(keys[i]->data, keys[i]->len)
                 << "] error:" << statuss[i].ToString();
    }

    results.push_back(false);
  }

  return results;
}

bool DiskDB::SetMeta(BufPtr key, BufPtr value) {
  return SetMetasDatas(KVPairS{std::make_pair(key, value)}, KVPairS());
}

bool DiskDB::SetMetas(const KVPairS &metas) {
  return SetMetasDatas(metas, KVPairS());
}

bool DiskDB::SetData(BufPtr key, BufPtr value) {
  return SetMetasDatas(KVPairS(), KVPairS{std::make_pair(key, value)});
}

bool DiskDB::SetDatas(const KVPairS &kvs) {
  return SetMetasDatas(KVPairS(), kvs);
}

bool DiskDB::SetMetaData(BufPtr mkey, BufPtr mvlaue, BufPtr dkey,
                         BufPtr dvalue) {
  return SetMetasDatas(KVPairS{std::make_pair(mkey, mvlaue)},
                       KVPairS{std::make_pair(dkey, dvalue)});
}

bool DiskDB::SetMetaDatas(BufPtr mkey, BufPtr mvlaue, const KVPairS &kvs) {
  return SetMetasDatas(KVPairS{std::make_pair(mkey, mvlaue)}, kvs);
}

bool DiskDB::SetMetasDatas(const KVPairS &metas, const KVPairS &kvs) {
  rocksdb::WriteBatch batch;
  for (auto iter = metas.begin(); iter != metas.end(); ++iter) {
    auto status = batch.Put(
        mt_handle_, rocksdb::Slice(iter->first->data, iter->first->len),
        rocksdb::Slice(iter->second->data, iter->second->len));
    if (!status.ok()) {
      LOG(ERROR) << "SetMetaDatas WriteBatch.Put:" << status.ToString();
      return false;
    }
  }

  for (auto iter = kvs.begin(); iter != kvs.end(); ++iter) {
    auto status = batch.Put(
        db_handle_, rocksdb::Slice(iter->first->data, iter->first->len),
        rocksdb::Slice(iter->second->data, iter->second->len));
    if (!status.ok()) {
      LOG(ERROR) << "SetMetaDatas WriteBatch.Put:" << status.ToString();
      return false;
    }
  }

  auto status = db_->Write(write_options_, &batch);
  if (!status.ok()) {
    LOG(ERROR) << "SetMetaDatas Write:" << status.ToString();
    return false;
  }

  return true;
}

void DiskDB::Compact() {
  LOG(INFO) << "Start to compct rocksdb:" << partition_name_;

  auto status = db_->CompactRange(rocksdb::CompactRangeOptions(), mt_handle_,
                                  nullptr, nullptr);
  if (!status.ok()) {
    LOG(ERROR) << "Compact partition:" << partition_name_
               << ", metadata error:" << status.ToString();
  }

  status = db_->CompactRange(rocksdb::CompactRangeOptions(), db_handle_,
                             nullptr, nullptr);
  if (!status.ok()) {
    LOG(ERROR) << "Compact partition:" << partition_name_
               << ", dbdata error:" << status.ToString();
  }
}

}  // namespace rockin