#pragma once
#include <uv.h>
#include <memory>
#include <vector>
#include "disk_db.h"
#include "mem_alloc.h"
#include "utils.h"

namespace rockin {
class DiskSaver {
 public:
  static DiskSaver *Default();
  static void DestoryDefault();

  DiskSaver();
  ~DiskSaver();

  // init disksaver
  void InitNoCrteate(int partition_num, const std::string &path);
  void InitAndCreate(int partition_num, const std::string &path);
  void InitAndCreate(int partition_num, std::vector<int> dbs,
                     const std::string &path);

  // get diskdb
  std::shared_ptr<DiskDB> GetDB(BufPtr key);

  void Compact();

 private:
  std::string path_;
  size_t partition_num_;
  std::vector<std::shared_ptr<DiskDB>> partitions_;
};

}  // namespace rockin