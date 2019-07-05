#include "event_loop.h"
#include <glog/logging.h>
#include <stdlib.h>
#include <iostream>

namespace rockin {
class SyncData {
 public:
  EventLoop::LoopCallback callback;
  std::shared_ptr<void> arg;
  uv_sem_t *sem;
};

EventLoop::EventLoop() : running_(false), queue_(0xF00000) {
  uv_loop_init(&loop_);
  loop_.data = this;
}

EventLoop::~EventLoop() {}

void EventLoop::Start() {
  if (running_) return;

  uv_thread_create(&thread_,
                   [](void *arg) {
                     EventLoop *st = (EventLoop *)arg;
                     st->RunLoop();
                   },
                   this);
}

void EventLoop::Stop() {
  this->running_ = false;
  this->RunInLoopAndWait(
      [](EventLoop *et, std::shared_ptr<void> arg) { uv_stop(&et->loop_); },
      nullptr);
}

void EventLoop::RunLoop() {
  this->running_ = true;
  async_.data = this;
  uv_async_init(&loop_, &async_, [](uv_async_t *handle) {
    EventLoop *lt = (EventLoop *)handle->data;
    lt->RunInLoop();
  });

  while (this->running_) {
    uv_run(&loop_, UV_RUN_DEFAULT);
  }
}

void EventLoop::RunInLoop() {
  while (true) {
    SyncData *sd = queue_.Pop();
    if (sd == nullptr) {
      return;
    }

    try {
      sd->callback(this, sd->arg);
    } catch (...) {
      LOG(ERROR) << "Catch Exception.";
    }
    if (sd->sem)
      uv_sem_post(sd->sem);
    else
      delete sd;
  }
}

void EventLoop::RunInLoopNoWait(LoopCallback callback,
                                std::shared_ptr<void> arg) {
  SyncData *sd = new SyncData;
  sd->callback = callback;
  sd->sem = nullptr;
  sd->arg = arg;

  while (!queue_.Push(sd))
    ;
  uv_async_send(&async_);
}

void EventLoop::RunInLoopAndWait(LoopCallback callback,
                                 std::shared_ptr<void> arg) {
  if (uv_thread_self() == thread_) {
    callback(this, arg);
    return;
  }

  uv_sem_t sem;
  uv_sem_init(&sem, 0);

  SyncData sd;
  sd.callback = callback;
  sd.sem = &sem;
  sd.arg = arg;

  while (!queue_.Push(&sd))
    ;
  uv_async_send(&async_);
  uv_sem_wait(&sem);
}

}  // namespace rockin