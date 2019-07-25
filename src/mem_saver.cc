#include "mem_saver.h"
#include <glog/logging.h>
#include <stdlib.h>
#include <mutex>
#include "dic_table.h"
#include "siphash.h"
#include "utils.h"

namespace rockin {
namespace {
std::once_flag once_flag;
MemSaver *g_data;
};  // namespace

MemSaver *MemSaver::Default() {
  std::call_once(once_flag, []() { g_data = new MemSaver(); });
  return g_data;
}

static uint64_t obj_key_hash(std::shared_ptr<object_t> obj) {
  return rockin::Hash((const uint8_t *)obj->key->data, obj->key->len);
}

static bool obj_key_equal(std::shared_ptr<object_t> a,
                          std::shared_ptr<object_t> b) {
  if ((a == nullptr || a->key == nullptr) &&
      (b == nullptr || b->key == nullptr))
    return true;

  if (a == nullptr || a->key == nullptr || b == nullptr || b->key == nullptr)
    return false;

  if (a->key->len != b->key->len) return false;
  for (size_t i = 0; i < a->key->len; i++) {
    if (*((char *)(a->key->data) + i) != *((char *)(b->key->data) + i)) {
      return false;
    }
  }
  return true;
}

static int obj_key_compare(std::shared_ptr<object_t> a,
                           std::shared_ptr<object_t> b) {
  if ((a == nullptr || a->key == nullptr) &&
      (b == nullptr || b->key == nullptr))
    return 0;
  if (a == nullptr || a->key == nullptr) return -1;
  if (b == nullptr || b->key == nullptr) return 1;

  for (size_t i = 0; i < a->key->len && i < b->key->len; i++) {
    uint8_t ac = *((uint8_t *)(a->key->data) + i);
    uint8_t bc = *((uint8_t *)(b->key->data) + i);
    if (ac > bc) return 1;
    if (ac < bc) return -1;
  }

  if (a->key->len > b->key->len) return 1;
  if (a->key->len < b->key->len) return -1;
  return 0;
}

MemSaver::MemSaver() { uv_key_create(&key_); }

MemSaver::~MemSaver() {}

bool MemSaver::Init() {
  void *arg = uv_key_get(&key_);
  if (arg == nullptr) {
    arg = new DicTable<object_t>(obj_key_equal, obj_key_hash);
  }
  return true;
}

// get obj from memsaver
ObjPtr MemSaver::GetObj(BufPtr key) { return nullptr; }

// get objs from memsaver
ObjPtrs MemSaver::GetObj(BufPtrs keys) { return ObjPtrs(); }

// insert obj to memsaver
void MemSaver::InsertObj(ObjPtr obj) {}

// insert obj to memsaver
void MemSaver::InsertObj(ObjPtrs obj) {
  //
}

void MemSaver::UpdateExpire(ObjPtr obj, uint64_t expire_ms) {}

}  // namespace rockin
