#pragma once
#include <rocksdb/db.h>
#include <iostream>
#include "rockin_alloc.h"

namespace rockin {

enum {
  Write_Meta,
  Write_Data,
  Del_Meta,
  Del_Data,
};

struct DiskWrite {
  int db;
  uint8_t type;
  MemPtr key;
  MemPtr value;

  DiskWrite(int db_, uint8_t type_, MemPtr key_, MemPtr value_)
      : db(db_), type(type_), key(key_), value(value_) {}
};

class DiskDB {
 public:
  DiskDB(int partion_id, const std::string partition_name,
         const std::string &path);
  ~DiskDB();

  bool WriteBatch(const std::vector<DiskWrite> &writes);

  bool GetMeta(int db, MemPtr key, std::string *value);
  bool GetData(int db, MemPtr key, std::string *value);
  std::vector<bool> GetDatas(int db, std::vector<MemPtr> &keys,
                             std::vector<std::string> *value);

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