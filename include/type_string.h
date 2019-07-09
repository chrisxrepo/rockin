#pragma once
#include <stdint.h>
#include <string.h>
#include <iostream>
#include "cmd_table.h"
#include "utils.h"

namespace rockin {
class RockinConn;
class CmdArgs;
class MemObj;

class StringCmd : public Cmd {
 public:
  StringCmd(CmdInfo info) : Cmd(info) {}
};

class StringCmd : public Cmd {
 public:
  std::string EncodeKey(std::shared_ptr<buffer_t> key);

  std::shared_ptr<buffer_t> DecodeKey(const std::string &key);
};

// GET key
class GetCmd : public StringCmd, public std::enable_shared_from_this<GetCmd> {
 public:
  GetCmd(CmdInfo info) : StringCmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// SET key value
class SetCmd : public StringCmd, public std::enable_shared_from_this<SetCmd> {
 public:
  SetCmd(CmdInfo info) : StringCmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// APPEND key value
class AppendCmd : public StringCmd,
                  public std::enable_shared_from_this<AppendCmd> {
 public:
  AppendCmd(CmdInfo info) : StringCmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// GETSET key value
class GetSetCmd : public StringCmd,
                  public std::enable_shared_from_this<GetSetCmd> {
 public:
  GetSetCmd(CmdInfo info) : StringCmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// MGET key1 [key2]...
class MGetCmd : public StringCmd, public std::enable_shared_from_this<MGetCmd> {
 public:
  MGetCmd(CmdInfo info) : StringCmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// MSET key1 value1 [kye2 value2]...
class MSetCmd : public StringCmd, public std::enable_shared_from_this<MSetCmd> {
 public:
  MSetCmd(CmdInfo info) : StringCmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// INCR key
class IncrCmd : public StringCmd, public std::enable_shared_from_this<IncrCmd> {
 public:
  IncrCmd(CmdInfo info) : StringCmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// INCRBY key value
class IncrbyCmd : public StringCmd,
                  public std::enable_shared_from_this<IncrbyCmd> {
 public:
  IncrbyCmd(CmdInfo info) : StringCmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// DECR key
class DecrCmd : public StringCmd, public std::enable_shared_from_this<DecrCmd> {
 public:
  DecrCmd(CmdInfo info) : StringCmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// DECR key value
class DecrbyCmd : public StringCmd,
                  public std::enable_shared_from_this<DecrbyCmd> {
 public:
  DecrbyCmd(CmdInfo info) : StringCmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// SETBIT key offset value
class SetBitCmd : public StringCmd,
                  public std::enable_shared_from_this<SetBitCmd> {
 public:
  SetBitCmd(CmdInfo info) : StringCmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// GETBIT key offset
class GetBitCmd : public StringCmd,
                  public std::enable_shared_from_this<GetBitCmd> {
 public:
  GetBitCmd(CmdInfo info) : StringCmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// BITCOUNT key [start end]
class BitCountCmd : public StringCmd,
                    public std::enable_shared_from_this<BitCountCmd> {
 public:
  BitCountCmd(CmdInfo info) : StringCmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// BITOP AND destkey srckey1 srckey2 srckey3 ... srckeyN
// BITOP OR destkey srckey1 srckey2 srckey3... srckeyN
// BITOP XOR destkey srckey1 srckey2 srckey3... srckeyN
// BITOP NOT destkey srckey
class BitopCmd : public StringCmd,
                 public std::enable_shared_from_this<BitopCmd> {
 public:
  BitopCmd(CmdInfo info) : StringCmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

// BITPOS key bit[start][end]
class BitPosCmd : public StringCmd,
                  public std::enable_shared_from_this<BitPosCmd> {
 public:
  BitPosCmd(CmdInfo info) : StringCmd(info) {}

  void Do(std::shared_ptr<CmdArgs> cmd_args,
          std::shared_ptr<RockinConn> conn) override;
};

}  // namespace rockin
