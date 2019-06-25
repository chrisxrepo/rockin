#include "redis_pool.h"
#include <glog/logging.h>
#include <random>
#include <thread>
#include "utils.h"

namespace rockin {

RedisPool *RedisPool::g_redis_pool_ = nullptr;

RedisPool *RedisPool::GetInstance() {
  if (g_redis_pool_ == nullptr) {
    g_redis_pool_ = new RedisPool();
  }

  return g_redis_pool_;
}

RedisPool::RedisPool() : thread_num_(0) {
  unsigned char seed[16];
  RandomBytes(seed, 16);
  hash_ = new SipHash(seed);
}

void RedisPool::Init(size_t thread_num) {
  thread_num_ = thread_num;

  for (int i = 0; i < thread_num; i++) {
    RedisDB *dic = new RedisDB();
    dbs_.push_back(dic);
  }

  for (int i = 0; i < thread_num; i++) {
    EventLoop *et = new EventLoop();
    et->Start();

    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    loops_.push_back(et);
  }
}

std::pair<EventLoop *, RedisDB *> RedisPool::GetDB(const std::string &key) {
  if (thread_num_ == 0) {
    return std::make_pair(nullptr, nullptr);
  }

  uint64_t h = hash_->Hash((const uint8_t *)key.c_str(), key.length());
  int idx = h % thread_num_;
  return std::make_pair(loops_[idx], dbs_[idx]);
}

}  // namespace rockin