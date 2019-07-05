#pragma once
#include <atomic>
#include <iostream>
#include "cmd_table.h"
#include "utils.h"

#define DBNum 16

namespace rockin {
class RockinConn;
class CmdArgs;

struct MultiResult {
  std::atomic<uint32_t> cnt;
  std::atomic<int64_t> int_value;
  std::vector<std::shared_ptr<buffer_t>> str_values;

  MultiResult(uint32_t cnt_) : cnt(cnt_), int_value(0), str_values(cnt_) {}
};

// COMMAND
class CommandCmd : public Cmd {
 public:
  CommandCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// PING
class PingCmd : public Cmd {
 public:
  PingCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// command
extern void CommandCommand(std::shared_ptr<CmdArgs> cmd);

// ping
extern void PingCommand(std::shared_ptr<CmdArgs> cmd);

// info
extern void InfoCommand(std::shared_ptr<CmdArgs> cmd);

// del key1 ...
extern void DelCommand(std::shared_ptr<CmdArgs> cmd);

// select dbnum
extern void SelectCommand(std::shared_ptr<CmdArgs> cmd);

// FLUSHDB
extern void FlushDBCommand(std::shared_ptr<CmdArgs> cmd);

// FLUSHALL
extern void FlushAllCommand(std::shared_ptr<CmdArgs> cmd);

}  // namespace rockin
