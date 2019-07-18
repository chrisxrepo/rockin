#pragma once
#include <rocksdb/db.h>
#include <iostream>
#include "mem_alloc.h"

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
  BufPtr key;
  BufPtr value;

  DiskWrite(int db_, uint8_t type_, BufPtr key_, BufPtr value_)
      : db(db_), type(type_), key(key_), value(value_) {}
};

class DiskDB {
 public:
  DiskDB(int partion_id, const std::string partition_name,
         const std::string &path);
  ~DiskDB();

  bool WriteBatch(const std::vector<DiskWrite> &writes);

  // get meta
  bool GetMeta(int db, BufPtr key, std::string *value);

  // get data
  bool GetData(int db, BufPtr key, std::string *value);

  // get multidata
  std::vector<bool> GetDatas(int db, std::vector<BufPtr> &keys,
                             std::vector<std::string> *value);

  // set meta
  bool SetMeta(int db, BufPtr key, BufPtr vlaue);

  // set multimeta
  bool SetMetas(int db, const KVPairS &metas);

  // set data
  bool SetData(int db, BufPtr key, BufPtr vlaue);

  // set multidata
  bool SetDatas(int db, const KVPairS &kvs);

  // set meta and data
  bool SetMetaData(int db, BufPtr mkey, BufPtr mvlaue, BufPtr dkey,
                   BufPtr dvlaue);

  // set meta and multidata
  bool SetMetaDatas(int db, BufPtr mkey, BufPtr mvlaue, const KVPairS &kvs);

  // set multimeta and multidata
  bool SetMetasDatas(int db, const KVPairS &metas, const KVPairS &kvs);

  // compact rocksdb
  void Compact();

 private:
  int partition_id_;
  std::string partition_name_;
  std::string path_;
  rocksdb::DB *db_;
  rocksdb::ReadOptions read_options_;
  rocksdb::WriteOptions write_options_;
  std::vector<rocksdb::ColumnFamilyHandle *> mt_handles_;
  std::vector<rocksdb::ColumnFamilyHandle *> db_handles_;

  static std::shared_ptr<rocksdb::Cache> g_meta_block_cache;
  static std::shared_ptr<rocksdb::Cache> g_data_block_cache;
};
}  // namespace rockin