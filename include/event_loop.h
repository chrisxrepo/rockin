#pragma once
#include <uv.h>
#include <queue>
#include "safe_queue.h"
#include "utils.h"

namespace rockin {
class SyncData;
class EventLoop {
 public:
  EventLoop();
  ~EventLoop();

  void Start();
  void Stop();

  uv_loop_t *loop() { return &loop_; }

  typedef std::function<void(EventLoop *lt, std::shared_ptr<void>)>
      LoopCallback;
  void RunInLoopNoWait(LoopCallback callback, std::shared_ptr<void> arg);
  void RunInLoopAndWait(LoopCallback callback, std::shared_ptr<void> arg);

 private:
  void RunLoop();
  void RunInLoop();

 private:
  uv_loop_t loop_;
  uv_thread_t thread_;
  bool running_;

  uv_async_t async_;
  SafeQueue<SyncData> queue_;
};
}  // namespace rockin
