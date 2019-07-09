#pragma once
#include <memory>
#include <vector>
#include "disk_db.h"
#include "rockin_alloc.h"
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
  std::shared_ptr<DiskDB> GetDB(std::shared_ptr<membuf_t> key);

 private:
  std::string path_;
  std::vector<std::shared_ptr<DiskDB>> dbs_;
};

}  // namespace rockin