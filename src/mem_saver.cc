#include "mem_saver.h"
#include <glog/logging.h>
#include <mutex>
#include <random>
#include <thread>
#include "mem_db.h"
#include "utils.h"

namespace rockin {

namespace {
std::once_flag mem_saver_once_flag;
MemSaver *g_mem_saver;
};  // namespace

MemSaver *MemSaver::Default() {
  std::call_once(mem_saver_once_flag, []() { g_mem_saver = new MemSaver(); });
  return g_mem_saver;
}

MemSaver::MemSaver() {
  unsigned char seed[16];
  RandomBytes(seed, 16);
  hash_ = new SipHash(seed);
}

void MemSaver::Init(size_t thread_num) {
  for (int i = 0; i < thread_num; i++) {
    EventLoop *et = new EventLoop();
    auto db = std::make_shared<MemDB>();
    dbs_.push_back(std::make_pair(et, db));

    et->Start();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
}

void MemSaver::DoCmd(std::shared_ptr<buffer_t> key,
                     EventLoop::LoopCallback cb) {
  uint64_t h = hash_->Hash((const uint8_t *)key->data, key->len);
  auto pair = dbs_[h % dbs_.size()];
  pair.first->RunInLoopNoWait(cb, pair.second);
}

std::pair<EventLoop *, MemDB *> MemSaver::GetDB(std::shared_ptr<buffer_t> key) {
  /* if (thread_num_ == 0) {
     return std::make_pair(nullptr, nullptr);
   }

   uint64_t h = hash_->Hash((const uint8_t *)key->data, key->len);
   int idx = h % thread_num_;
   return dbs_[idx];
   */
  return std::make_pair(nullptr, nullptr);
}

std::vector<std::pair<EventLoop *, MemDB *>> MemSaver::GetDBs() {
  std::vector<std::pair<EventLoop *, MemDB *>> dbs;
  /* for (int i = 0; i < loops_.size(); i++) {
     dbs.push_back(std::make_pair(loops_[i], dbs_[i]));
   }*/
  return dbs;
}

}  // namespace rockin