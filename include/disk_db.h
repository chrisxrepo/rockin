#pragma once
#include <rocksdb/db.h>
#include <iostream>

namespace rockin {
class DiskDB {
 public:
  DiskDB(int partion_id, const std::string partition_name,
         const std::string &path);
  ~DiskDB();

  bool GetMeta(int db, const std::string &key, std::string *value);
  bool GetData(int db, const std::string &key, std::string *value);

  bool SetMeta(int db, const std::string &key, const std::string &value);
  bool SetData(int db, const std::string &key, const std::string &value);
  bool SetAll(int db,
              const std::vector<std::pair<std::string, std::string>> &metas,
              const std::vector<std::pair<std::string, std::string>> &datas);

 private:
  bool Get(rocksdb::ColumnFamilyHandle *cf, const std::string &key,
           std::string *value);
  bool Set(rocksdb::ColumnFamilyHandle *cf, const std::string &key,
           const std::string &value);

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