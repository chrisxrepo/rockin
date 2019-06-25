#pragma once
#include <uv.h>
#include <iostream>
#include <vector>
#include "event_loop.h"

namespace rockin {

class NetPool {
 public:
  static NetPool *GetInstance();

  void Init(int thread_num);

  bool ListenSocket(
      int sock, std::function<void(EventLoop *, uv_stream_t *, int)> callback);

  void Stop();

 private:
  std::vector<EventLoop *> loops_;

 private:
  static NetPool *g_net_pool_;
};
}  // namespace rockin