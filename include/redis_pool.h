#pragma once
#include <uv.h>
#include <iostream>
#include <vector>
#include "event_loop.h"
#include "redis_db.h"
#include "siphash.h"

namespace rockin {

class RedisPool {
 public:
  RedisPool();
  static RedisPool *GetInstance();

  void Init(size_t thread_num);

  std::pair<EventLoop *, RedisDB *> GetDB(std::shared_ptr<buffer_t> key);
  std::vector<std::pair<EventLoop *, RedisDB *>> GetDBs();

 private:
  SipHash *hash_;
  std::vector<EventLoop *> loops_;
  std::vector<RedisDB *> dbs_;
  size_t thread_num_;

 private:
  static RedisPool *g_redis_pool_;
};
}  // namespace rockin