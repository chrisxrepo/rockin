#pragma once
#include <stdint.h>
#include <string.h>
#include <iostream>
#include "cmd_table.h"
#include "disk_saver.h"
#include "utils.h"

namespace rockin {
class RockinConn;
class CmdArgs;
class MemObj;

class StringCmd : public Cmd {
 public:
  StringCmd(CmdInfo info) : Cmd(info) {}

  // get meta from disk saver
  std::shared_ptr<MemObj> GetMeta(int dbindex, MemPtr key, uint16_t &bulk);

  // get obj from memsaver
  // if not exist then get obj from disksaver
  std::shared_ptr<MemObj> GetObj(int dbindex, std::shared_ptr<MemDB> db,
                                 MemPtr key, bool &type_err, uint32_t &version);

  // add obj
  std::shared_ptr<MemObj> AddObj(std::shared_ptr<MemDB> db, int dbindex,
                                 MemPtr key, MemPtr value, int type, int encode,
                                 uint32_t version);

  std::shared_ptr<MemObj> UpdateObj(int dbindex, std::shared_ptr<MemObj> obj,
                                    MemPtr value, int type, int encode,
                                    int old_bulk);

 private:
  // udpate string
  bool Update(int dbindex, std::shared_ptr<MemObj> obj, bool update_meta);
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

 private:
  MemPtr DoSetBit(MemPtr value, int64_t offset, int on, int &ret);
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
