#pragma once
#include <rocksdb/db.h>
#include <iostream>

namespace rockin {
class RocksDB {
 public:
  RocksDB(const std::string &path, const rocksdb::Options options);
  ~RocksDB();

 private:
  std::string path_;
  rocksdb::DB *db_;
};
}  // namespace rockin