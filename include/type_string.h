#pragma once
#include <stdint.h>
#include <string.h>
#include <iostream>
#include "disk_saver.h"
#include "utils.h"
#include "workers.h"

namespace rockin {
class RockinConn;
class CmdArgs;
class object_t;

// GET key
class GetCmd : public Cmd, public std::enable_shared_from_this<GetCmd> {
 public:
  GetCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// SET key value [EX seconds|PX milliseconds] [NX|XX]
class SetCmd : public Cmd, public std::enable_shared_from_this<SetCmd> {
 public:
  SetCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// APPEND key value
class AppendCmd : public Cmd, public std::enable_shared_from_this<AppendCmd> {
 public:
  AppendCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// GETSET key value
class GetSetCmd : public Cmd, public std::enable_shared_from_this<GetSetCmd> {
 public:
  GetSetCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// MGET key1 [key2]...
class MGetCmd : public Cmd, public std::enable_shared_from_this<MGetCmd> {
 public:
  MGetCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// MSET key1 value1 [kye2 value2]...
class MSetCmd : public Cmd, public std::enable_shared_from_this<MSetCmd> {
 public:
  MSetCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// INCR key
class IncrCmd : public Cmd, public std::enable_shared_from_this<IncrCmd> {
 public:
  IncrCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// INCRBY key value
class IncrbyCmd : public Cmd, public std::enable_shared_from_this<IncrbyCmd> {
 public:
  IncrbyCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// DECR key
class DecrCmd : public Cmd, public std::enable_shared_from_this<DecrCmd> {
 public:
  DecrCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// DECR key value
class DecrbyCmd : public Cmd, public std::enable_shared_from_this<DecrbyCmd> {
 public:
  DecrbyCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// SETBIT key offset value
class SetBitCmd : public Cmd, public std::enable_shared_from_this<SetBitCmd> {
 public:
  SetBitCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// GETBIT key offset
class GetBitCmd : public Cmd, public std::enable_shared_from_this<GetBitCmd> {
 public:
  GetBitCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// BITCOUNT key [start end]
class BitCountCmd : public Cmd,
                    public std::enable_shared_from_this<BitCountCmd> {
 public:
  BitCountCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// BITOP AND destkey srckey1 srckey2 srckey3 ... srckeyN
// BITOP OR destkey srckey1 srckey2 srckey3... srckeyN
// BITOP XOR destkey srckey1 srckey2 srckey3... srckeyN
// BITOP NOT destkey srckey
class BitopCmd : public Cmd, public std::enable_shared_from_this<BitopCmd> {
 public:
  BitopCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// BITPOS key bit[start][end]
class BitPosCmd : public Cmd, public std::enable_shared_from_this<BitPosCmd> {
 public:
  BitPosCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

class StringDebug : public Cmd,
                    public std::enable_shared_from_this<StringDebug> {
 public:
  StringDebug(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

}  // namespace rockin
