#pragma once
#include <uv.h>
#include <iostream>
#include <vector>
#include "event_loop.h"
#include "siphash.h"

namespace rockin {
class EventLoop;
class MemDB;

class MemSaver {
 public:
  MemSaver();
  static MemSaver *Default();

  void Init(size_t thread_num);

  void DoCmd(std::shared_ptr<buffer_t> key, EventLoop::LoopCallback cb);

  std::pair<EventLoop *, MemDB *> GetDB(std::shared_ptr<buffer_t> key);
  std::vector<std::pair<EventLoop *, MemDB *>> GetDBs();

 private:
  SipHash *hash_;
  std::vector<std::pair<EventLoop *, std::shared_ptr<MemDB>>> dbs_;
};
}  // namespace rockin