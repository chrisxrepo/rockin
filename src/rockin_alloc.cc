#include "rockin_alloc.h"
#include <mutex>

namespace rockin {
namespace {
std::once_flag mem_alloc_once_flag;
std::atomic<uint64_t> g_mem_size;
};  // namespace

uint64_t change_size(size_t size) {
  std::call_once(mem_alloc_once_flag, []() { g_mem_size = 0; });
  g_mem_size.fetch_add(size);
  return g_mem_size.load();
}

}  // namespace rockin