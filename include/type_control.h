#pragma once
#include <atomic>
#include <iostream>
#include "utils.h"
#include "workers.h"

namespace rockin {
class RockinConn;
class CmdArgs;

struct MultiResult {
  std::atomic<uint32_t> cnt;
  std::atomic<bool> error;
  std::atomic<int64_t> int_value;
  std::vector<BufPtr> str_values;

  MultiResult(uint32_t cnt_)
      : cnt(cnt_), error(false), int_value(0), str_values(cnt_) {}
};

// COMMAND
class CommandCmd : public Cmd, public std::enable_shared_from_this<CommandCmd> {
 public:
  CommandCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// PING
class PingCmd : public Cmd, public std::enable_shared_from_this<PingCmd> {
 public:
  PingCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// INFO
class InfoCmd : public Cmd, public std::enable_shared_from_this<InfoCmd> {
 public:
  InfoCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// DEL key1 [key2]...
class DelCmd : public Cmd, public std::enable_shared_from_this<DelCmd> {
 public:
  DelCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// SELECT dbnum
class SelectCmd : public Cmd, public std::enable_shared_from_this<SelectCmd> {
 public:
  SelectCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// FLUSHDB
class FlushDBCmd : public Cmd, public std::enable_shared_from_this<FlushDBCmd> {
 public:
  FlushDBCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// FLUSHALL
class FlushAllCmd : public Cmd,
                    public std::enable_shared_from_this<FlushAllCmd> {
 public:
  FlushAllCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// TTL key
class TTLCmd : public Cmd, public std::enable_shared_from_this<TTLCmd> {
 public:
  TTLCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// PTTL key
class PTTLCmd : public Cmd, public std::enable_shared_from_this<PTTLCmd> {
 public:
  PTTLCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// EXPIRE key seconds
class ExpireCmd : public Cmd, public std::enable_shared_from_this<ExpireCmd> {
 public:
  ExpireCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// PEXPIRE key milliseconds
class PExpireCmd : public Cmd, public std::enable_shared_from_this<PExpireCmd> {
 public:
  PExpireCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// EXPIREAT key timestamp
class ExpireAtCmd : public Cmd,
                    public std::enable_shared_from_this<ExpireAtCmd> {
 public:
  ExpireAtCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// PEXPIREAT key millisecond-timestamp
class PExpireAtCmd : public Cmd,
                     public std::enable_shared_from_this<PExpireAtCmd> {
 public:
  PExpireAtCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// COMPACT
class CompactCmd : public Cmd, public std::enable_shared_from_this<CompactCmd> {
 public:
  CompactCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};
}  // namespace rockin
