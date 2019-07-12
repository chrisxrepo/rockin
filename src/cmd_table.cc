#include "cmd_table.h"
#include <glog/logging.h>
#include <algorithm>
#include <mutex>
#include <sstream>
#include "cmd_args.h"
#include "rockin_conn.h"
#include "type_common.h"
#include "type_string.h"

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
  // COMMAND
  auto command_ptr = std::make_shared<CommandCmd>(CmdInfo("command", 1));
  cmd_table_.insert(std::make_pair("command", command_ptr));

  // PING
  auto ping_ptr = std::make_shared<PingCmd>(CmdInfo("ping", 1));
  cmd_table_.insert(std::make_pair("ping", ping_ptr));

  // INFO
  auto info_ptr = std::make_shared<InfoCmd>(CmdInfo("info", 1));
  cmd_table_.insert(std::make_pair("info", info_ptr));

  // DEL key1 [key2]...
  auto del_ptr = std::make_shared<DelCmd>(CmdInfo("del", -2));
  cmd_table_.insert(std::make_pair("del", del_ptr));

  // SELECT dbnum
  auto select_ptr = std::make_shared<SelectCmd>(CmdInfo("select", 2));
  cmd_table_.insert(std::make_pair("select", select_ptr));

  // FLUSHDB
  auto flushdb_ptr = std::make_shared<FlushDBCmd>(CmdInfo("flushdb", 1));
  cmd_table_.insert(std::make_pair("flushdb", flushdb_ptr));

  // FLUSHALL
  auto flushall_ptr = std::make_shared<FlushAllCmd>(CmdInfo("flushall", 1));
  cmd_table_.insert(std::make_pair("flushall", flushall_ptr));

  // GET key
  auto get_ptr = std::make_shared<GetCmd>(CmdInfo("get", 2));
  cmd_table_.insert(std::make_pair("get", get_ptr));

  auto set_ptr = std::make_shared<SetCmd>(CmdInfo("set", 3));
  cmd_table_.insert(std::make_pair("set", set_ptr));

  // APPEND key value
  auto append_ptr = std::make_shared<AppendCmd>(CmdInfo("append", 3));
  cmd_table_.insert(std::make_pair("append", append_ptr));

  // GETSET key value
  auto getset_ptr = std::make_shared<GetSetCmd>(CmdInfo("getset", 3));
  cmd_table_.insert(std::make_pair("getset", getset_ptr));

  // MGET key1 [key2]...
  auto mget_ptr = std::make_shared<MGetCmd>(CmdInfo("mget", -2));
  cmd_table_.insert(std::make_pair("mget", mget_ptr));

  // MSET key1 value1 [kye2 value2]...
  auto mset_ptr = std::make_shared<MSetCmd>(CmdInfo("mset", -3));
  cmd_table_.insert(std::make_pair("mset", mset_ptr));

  // INCR key
  auto incr_ptr = std::make_shared<IncrCmd>(CmdInfo("incr", 2));
  cmd_table_.insert(std::make_pair("incr", incr_ptr));

  // INCRBY key value
  auto incrby_ptr = std::make_shared<IncrbyCmd>(CmdInfo("incrby", 3));
  cmd_table_.insert(std::make_pair("incrby", incrby_ptr));

  // DECR key
  auto decr_ptr = std::make_shared<DecrCmd>(CmdInfo("decr", 2));
  cmd_table_.insert(std::make_pair("decr", decr_ptr));

  // DECR key value
  auto decrby_ptr = std::make_shared<DecrbyCmd>(CmdInfo("decrby", 3));
  cmd_table_.insert(std::make_pair("decrby", decrby_ptr));

  // SETBIT key offset value
  auto setbit_ptr = std::make_shared<SetBitCmd>(CmdInfo("setbit", 4));
  cmd_table_.insert(std::make_pair("setbit", setbit_ptr));

  // GETBIT key offset
  auto getbit_ptr = std::make_shared<GetBitCmd>(CmdInfo("getbit", 3));
  cmd_table_.insert(std::make_pair("getbit", getbit_ptr));

  // BITCOUNT key [start end]
  auto bitcount_ptr = std::make_shared<BitCountCmd>(CmdInfo("bitcount", -2));
  cmd_table_.insert(std::make_pair("bitcount", bitcount_ptr));

  // BITOP AND destkey srckey1 srckey2 srckey3 ... srckeyN
  // BITOP OR destkey srckey1 srckey2 srckey3... srckeyN
  // BITOP XOR destkey srckey1 srckey2 srckey3... srckeyN
  // BITOP NOT destkey srckey
  auto bitop_ptr = std::make_shared<BitopCmd>(CmdInfo("bitop", -4));
  cmd_table_.insert(std::make_pair("bitop", bitop_ptr));

  // BITPOS key bit[start][end]
  auto bitpos_ptr = std::make_shared<BitPosCmd>(CmdInfo("bitpos", -3));
  cmd_table_.insert(std::make_pair("bitpos", bitpos_ptr));
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
    conn->ReplyError(rockin::make_shared<membuf_t>(build.str()));
    return;
  }

  if ((iter->second->info().arity > 0 &&
       iter->second->info().arity != args.size()) ||
      int(args.size()) < -iter->second->info().arity) {
    std::ostringstream build;
    build << "ERR wrong number of arguments for '" << cmd << "' command";
    conn->ReplyError(rockin::make_shared<membuf_t>(build.str()));
    return;
  }

  iter->second->Do(cmd_args, conn);
}

}  // namespace rockin