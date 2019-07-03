#include "rocks_db.h"
#include <assert.h>
#include <glog/logging.h>
#include "utils.h"

namespace rockin {
RocksDB::RocksDB(const std::string &path, const rocksdb::Options options)
    : path_(path) {
  auto status = rocksdb::DB::Open(options, path, &db_);
  assert(status.ok());

  LOG(INFO) << "open rocksdb:" << path
            << ", rocksdb size:" << GetSizeString(GetDirectorySize(path));
}

RocksDB::~RocksDB() {
  if (db_) {
    delete db_;
  }
}
}  // namespace rockin