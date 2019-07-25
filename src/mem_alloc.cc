#include "mem_alloc.h"
#include <atomic>
#include <mutex>

namespace rockin {
namespace {
std::once_flag mem_alloc_once_flag;
std::atomic<uint64_t> g_mem_size;
};  // namespace

void change_momory_size(size_t size) {
  std::call_once(mem_alloc_once_flag, []() { g_mem_size = 0; });
  g_mem_size.fetch_add(size);
}

uint64_t get_memory_size() {
  std::call_once(mem_alloc_once_flag, []() { g_mem_size = 0; });
  return g_mem_size.load();
}

}  // namespace rockin