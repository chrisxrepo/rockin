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

bool DiskDB::WriteBatch(const std::vector<DiskWrite> &writes) {
  if (writes.size() == 0) {
    return true;
  }

  rocksdb::WriteBatch batch;
  for (auto iter = writes.begin(); iter != writes.end(); ++iter) {
    if (iter->db < 0 || iter->db >= mt_handles_.size()) {
      LOG(ERROR) << "dbnum invalid:" << iter->db
                 << ", key:" << std::string(iter->key->data, iter->key->len)
                 << ", value:"
                 << (iter->value == nullptr
                         ? ""
                         : std::string(iter->value->data, iter->value->len));
      continue;
    }

    if (iter->type == Write_Meta) {
      batch.Put(mt_handles_[iter->db],
                rocksdb::Slice(iter->key->data, iter->key->len),
                rocksdb::Slice(iter->value->data, iter->value->len));
    } else if (iter->type == Write_Data) {
      batch.Put(db_handles_[iter->db],
                rocksdb::Slice(iter->key->data, iter->key->len),
                rocksdb::Slice(iter->value->data, iter->value->len));
    } else if (iter->type == Del_Meta) {
      batch.Delete(mt_handles_[iter->db],
                   rocksdb::Slice(iter->key->data, iter->key->len));
    } else if (iter->type == Del_Data) {
      batch.Delete(db_handles_[iter->db],
                   rocksdb::Slice(iter->key->data, iter->key->len));
    } else {
      LOG(WARNING) << "WriteBatch unknow type:" << iter->type;
    }
  }

  auto status = db_->Write(write_options_, &batch);
  if (!status.ok()) {
    LOG(ERROR) << "rocksdb Write:" << status.ToString();
  }

  return status.ok();
}

bool DiskDB::GetMeta(int db, MemPtr key, std::string *value) {
  if (db < 0 || db >= mt_handles_.size()) {
    LOG(ERROR) << "GetMeta dbnum invalid:" << db
               << ", key:" << std::string(key->data, key->len);
    return false;
  }

  auto status = db_->Get(read_options_, mt_handles_[db],
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

bool DiskDB::GetData(int db, MemPtr key, std::string *value) {
  if (db < 0 || db >= db_handles_.size()) {
    LOG(ERROR) << "GetData dbnum invalid:" << db
               << ", key:" << std::string(key->data, key->len);
    return false;
  }

  auto status = db_->Get(read_options_, db_handles_[db],
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

std::vector<bool> DiskDB::GetDatas(int db, std::vector<MemPtr> &keys,
                                   std::vector<std::string> *values) {
  if (db < 0 || db >= db_handles_.size()) {
    LOG(ERROR) << "GetDatas dbnum invalid:" << db
               << ", keys num:" << keys.size();
    return std::vector<bool>(keys.size());
  }

  std::vector<rocksdb::Slice> slices;
  std::vector<rocksdb::ColumnFamilyHandle *> cfs;
  for (size_t i = 0; i < keys.size(); i++) {
    cfs.push_back(db_handles_[db]);
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

bool DiskDB::SetMeta(int db, MemPtr key, MemPtr value) {
  return SetMetasDatas(db, KVPairS{std::make_pair(key, value)}, KVPairS());
}

bool DiskDB::SetMetas(int db, const KVPairS &metas) {
  return SetMetasDatas(db, metas, KVPairS());
}

bool DiskDB::SetData(int db, MemPtr key, MemPtr value) {
  return SetMetasDatas(db, KVPairS(), KVPairS{std::make_pair(key, value)});
}

bool DiskDB::SetDatas(int db, const KVPairS &kvs) {
  return SetMetasDatas(db, KVPairS(), kvs);
}

bool DiskDB::SetMetaData(int db, MemPtr mkey, MemPtr mvlaue, MemPtr dkey,
                         MemPtr dvalue) {
  return SetMetasDatas(db, KVPairS{std::make_pair(mkey, mvlaue)},
                       KVPairS{std::make_pair(dkey, dvalue)});
}

bool DiskDB::SetMetaDatas(int db, MemPtr mkey, MemPtr mvlaue,
                          const KVPairS &kvs) {
  return SetMetasDatas(db, KVPairS{std::make_pair(mkey, mvlaue)}, kvs);
}

bool DiskDB::SetMetasDatas(int db, const KVPairS &metas, const KVPairS &kvs) {
  if (db < 0 || db >= DBNum) {
    LOG(ERROR) << "SetMetaDatas dbnum invalid:" << db;
    return false;
  }

  rocksdb::WriteBatch batch;
  for (auto iter = metas.begin(); iter != metas.end(); ++iter) {
    auto status = batch.Put(
        mt_handles_[db], rocksdb::Slice(iter->first->data, iter->first->len),
        rocksdb::Slice(iter->second->data, iter->second->len));
    if (!status.ok()) {
      LOG(ERROR) << "SetMetaDatas WriteBatch.Put:" << status.ToString();
      return false;
    }
  }

  for (auto iter = kvs.begin(); iter != kvs.end(); ++iter) {
    auto status = batch.Put(
        db_handles_[db], rocksdb::Slice(iter->first->data, iter->first->len),
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

}  // namespace rockin