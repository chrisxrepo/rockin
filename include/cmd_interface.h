#pragma once
#include <iostream>
#include "coding.h"
#include "mem_db.h"
#include "utils.h"

namespace rockin {
class CmdArgs;
class RockinConn;

struct CmdInfo {
  std::string name;
  int arity;

  CmdInfo() : arity(0) {}
  CmdInfo(std::string name_, int arity_) : name(name_), arity(arity_) {}
};

class Cmd {
 public:
  Cmd(CmdInfo info) : info_(info) {}

  virtual void Do(std::shared_ptr<CmdArgs> cmd_args,
                  std::shared_ptr<RockinConn> conn) = 0;

  // meta key ->  key
  // meta value-> |  version  |   ttl   |  type  |  encode  |
  //              |   2 byte  |  4 byte | 1 byte |  1 byte  |
  virtual std::shared_ptr<membuf_t> MetaValue(std::shared_ptr<MemObj> obj) {
    std::shared_ptr<membuf_t> v = rockin::make_shared<membuf_t>(8);
    EncodeFixed16(v->data, 100);
    EncodeFixed32(v->data + 2, 0);
    EncodeFixed8(v->data + 6, obj->type);
    EncodeFixed8(v->data + 7, obj->encode);
    return v;
  }

  const CmdInfo &info() { return info_; }

 private:
  CmdInfo info_;
};

}  // namespace rockin