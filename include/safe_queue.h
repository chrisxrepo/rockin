#pragma once
#include <pthread.h>
#include <uv.h>
#include "stdlib.h"
#include "utils.h"

namespace rockin {

class SpinLock {
 public:
  SpinLock() { uv_mutex_init(&lock_); }

  void Lock() {
    for (int i = 0; i < 1000; i++) {
      if (uv_mutex_trylock(&lock_) == 0) return;
    }
    uv_mutex_lock(&lock_);
  }

  void UnLock() { uv_mutex_unlock(&lock_); }

 private:
  uv_mutex_t lock_;
};

template <typename T>
class SafeQueue {
 public:
  SafeQueue(size_t size) {
    size_ = NextPower(size);
    queue_ = (T**)malloc(sizeof(T*) * size_);
    tail_ = head_ = 0;
    mask_ = size_ - 1;
  }

  bool Push(T* d) {
    if ((head_ & mask_) - (tail_ & mask_) >= size_) {
      return false;
    }

    lock_.Lock();
    queue_[head_ & mask_] = d;
    head_++;
    lock_.UnLock();

    return true;
  }

  T* Pop() {
    if ((tail_ & mask_) >= (head_ & mask_)) {
      return nullptr;
    }

    lock_.Lock();
    T* d = queue_[tail_ & mask_];
    tail_++;
    lock_.UnLock();

    return d;
  }

 private:
  size_t size_, mask_;
  volatile size_t tail_, head_;
  T** queue_;
  SpinLock lock_;
};
}  // namespace rockin
