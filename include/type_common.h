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

// INFO
class InfoCmd : public Cmd {
 public:
  InfoCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// DEL key1 [key2]...
class DelCmd : public Cmd {
 public:
  DelCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// SELECT dbnum
class SelectCmd : public Cmd {
 public:
  SelectCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// FLUSHDB
class FlushDBCmd : public Cmd {
 public:
  FlushDBCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// FLUSHALL
class FlushAllCmd : public Cmd {
 public:
  FlushAllCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

}  // namespace rockin
