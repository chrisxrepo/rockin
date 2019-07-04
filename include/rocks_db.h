#pragma once
#include <rocksdb/db.h>
#include <iostream>

namespace rockin {
class RocksDB {
 public:
  RocksDB(int partion_id, const std::string partition_name,
          const std::string &path);
  ~RocksDB();

 private:
  int partition_id_;
  std::string partition_name_;
  std::string path_;
  rocksdb::DB *db_;
  std::vector<rocksdb::ColumnFamilyHandle *> mt_handles_;
  std::vector<rocksdb::ColumnFamilyHandle *> db_handles_;

  static std::shared_ptr<rocksdb::Cache> g_meta_block_cache;
  static std::shared_ptr<rocksdb::Cache> g_data_block_cache;
};
}  // namespace rockin