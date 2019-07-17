#include "mem_saver.h"
#include <glog/logging.h>
#include <uv.h>
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
    auto db = std::make_shared<MemDB>();
    EventLoop *et = new EventLoop();
    dbs_.push_back(std::make_pair(et, db));

    /* uv_timer_t *reshah_timer = (uv_timer_t *)malloc(sizeof(uv_timer_t));
    uv_timer_init(et->loop(), reshah_timer);
    reshah_timer->data = malloc(sizeof(i));
    *((int *)reshah_timer->data) = i;
    uv_timer_start(reshah_timer,
                   [](uv_timer_t *t) {
                     auto db =
                         MemSaver::Default()->dbs_[*((int *)t->data)].second;
                     db->RehashTimer(GetMilliSec());
                   },
                   10, 100);

    uv_timer_t *expire_timer = (uv_timer_t *)malloc(sizeof(uv_timer_t));
    uv_timer_init(et->loop(), expire_timer);
    expire_timer->data = malloc(sizeof(i));
    *((int *)expire_timer->data) = i;
    uv_timer_start(expire_timer,
                   [](uv_timer_t *t) {
                     auto db =
                         MemSaver::Default()->dbs_[*((int *)t->data)].second;
                     db->ExpireTimer(GetMilliSec());
                   },
                   50, 100);

    et->Start();
*/
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
}

void MemSaver::DoCmd(MemPtr key, EventLoop::LoopCallback callback) {
  uint64_t h = hash_->Hash((const uint8_t *)key->data, key->len);
  auto pair = dbs_[h % dbs_.size()];
  // pair.first->RunInLoopNoWait(cb, pair.second);
  callback(nullptr, pair.second);
}

}  // namespace rockin