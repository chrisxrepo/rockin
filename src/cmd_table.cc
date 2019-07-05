#include "cmd_table.h"
#include <mutex>

namespace rockin {

namespace {
std::once_flag cmd_table_once_flag;
CmdTable* g_cmd_table;
};  // namespace

CmdTable* CmdTable::Default() {
  std::call_once(cmd_table_once_flag, []() { g_cmd_table = new CmdTable(); });
  return g_cmd_table;
}

void Init() {}

}  // namespace rockin