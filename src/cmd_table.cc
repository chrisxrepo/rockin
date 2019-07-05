#include "cmd_table.h"
#include <glog/logging.h>
#include <algorithm>
#include <mutex>
#include <sstream>
#include "cmd_args.h"
#include "rockin_conn.h"
#include "type_common.h"

namespace rockin {

namespace {
std::once_flag cmd_table_once_flag;
CmdTable* g_cmd_table;
};  // namespace

CmdTable* CmdTable::Default() {
  std::call_once(cmd_table_once_flag, []() { g_cmd_table = new CmdTable(); });
  return g_cmd_table;
}

void CmdTable::Init() {
  auto commandptr = std::make_shared<CommandCmd>(CmdInfo("command", 1));
  cmd_table_.insert(std::make_pair("command", commandptr));

  auto pingptr = std::make_shared<PingCmd>(CmdInfo("ping", 1));
  cmd_table_.insert(std::make_pair("ping", pingptr));
}

void CmdTable::HandeCmd(std::shared_ptr<RockinConn> conn,
                        std::shared_ptr<CmdArgs> cmd_args) {
  auto& args = cmd_args->args();

  if (args.empty()) return;

  std::string cmd(args[0]->data, args[0]->len);
  std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::tolower);
  auto iter = cmd_table_.find(cmd);
  if (iter == cmd_table_.end()) {
    std::ostringstream build;
    build << "ERR unknown command `" << cmd << "`, with args beginning with: ";
    for (int i = 1; i < args.size(); i++) build << "`" << args[i] << "`, ";
    conn->ReplyError(make_buffer(build.str()));
    return;
  }

  if ((iter->second->info().arity > 0 &&
       iter->second->info().arity != args.size()) ||
      int(args.size()) < -iter->second->info().arity) {
    std::ostringstream build;
    build << "ERR wrong number of arguments for '" << cmd << "' command";
    conn->ReplyError(make_buffer(build.str()));
    return;
  }

  iter->second->Do(cmd_args, conn);
}

}  // namespace rockin