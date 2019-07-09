#pragma once
#include <uv.h>
#include <memory>
#include <vector>
#include "disk_db.h"
#include "rockin_alloc.h"
#include "safe_queue.h"
#include "utils.h"

namespace rockin {
class _WriteData;

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
  std::shared_ptr<DiskDB> GetDB(MemPtr key);

  typedef std::vector<std::pair<MemPtr, MemPtr>> WriteKVS;
  bool WriteData(int dbindex, MemPtr key, WriteKVS mates, WriteKVS datas);

 private:
  void RunLoop();
  void WriteDiskData();

 private:
  std::string path_;
  std::vector<std::shared_ptr<DiskDB>> partitions_;
  uv_loop_t write_loop_;
  uv_thread_t write_thread_;
  uv_async_t write_async_;
  SafeQueue<_WriteData> write_queue_;
};

}  // namespace rockin