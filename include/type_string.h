#pragma once
#include <stdint.h>
#include <string.h>
#include <iostream>
#include "cmd_table.h"
#include "utils.h"

namespace rockin {
class RockinConn;
class CmdArgs;

// GET key
class GetCmd : public Cmd {
 public:
  GetCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// SET key value
class SetCmd : public Cmd {
 public:
  SetCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// APPEND key value
class AppendCmd : public Cmd {
 public:
  AppendCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// GETSET key value
class GetSetCmd : public Cmd {
 public:
  GetSetCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// MGET key1 [key2]...
class MGetCmd : public Cmd {
 public:
  MGetCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// MSET key1 value1 [kye2 value2]...
class MSetCmd : public Cmd {
 public:
  MSetCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// INCR key
class IncrCmd : public Cmd {
 public:
  IncrCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// INCRBY key value
class IncrbyCmd : public Cmd {
 public:
  IncrbyCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// DECR key
class DecrCmd : public Cmd {
 public:
  DecrCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// DECR key value
class DecrbyCmd : public Cmd {
 public:
  DecrbyCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// SETBIT key offset value
class SetBitCmd : public Cmd {
 public:
  SetBitCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// GETBIT key offset
class GetBitCmd : public Cmd {
 public:
  GetBitCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// BITCOUNT key [start end]
class BitCountCmd : public Cmd {
 public:
  BitCountCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// BITOP AND destkey srckey1 srckey2 srckey3 ... srckeyN
// BITOP OR destkey srckey1 srckey2 srckey3... srckeyN
// BITOP XOR destkey srckey1 srckey2 srckey3... srckeyN
// BITOP NOT destkey srckey
class BitopCmd : public Cmd {
 public:
  BitopCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// BITPOS key bit[start][end]
class BitPosCmd : public Cmd {
 public:
  BitPosCmd(CmdInfo info) : Cmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

}  // namespace rockin
