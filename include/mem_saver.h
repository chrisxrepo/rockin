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

  const std::vector<std::pair<EventLoop *, std::shared_ptr<MemDB>>> &dbs() {
    return dbs_;
  }

 private:
  SipHash *hash_;
  std::vector<std::pair<EventLoop *, std::shared_ptr<MemDB>>> dbs_;
};
}  // namespace rockin