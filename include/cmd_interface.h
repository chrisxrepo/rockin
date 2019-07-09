#pragma once
#include <iostream>
#include "coding.h"
#include "disk_saver.h"
#include "mem_db.h"
#include "utils.h"

#define META_TYPE(str, len) DecodeFixed8((const char *)(str) + ((len)-10))
#define META_ENCODE(str, len) DecodeFixed8((const char *)(str) + ((len)-9))
#define META_VERSION(str, len) DecodeFixed32((const char *)(str) + ((len)-8))
#define META_TTL(str, len) DecodeFixed32((const char *)(str) + ((len)-4))

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

  std::shared_ptr<MemObj> GetBaseMeta(int dbindex, MemPtr key,
                                      std::string *meta_value) {
    auto diskdb = DiskSaver::Default()->GetDB(key);
    if (diskdb->GetMeta(dbindex, key, meta_value) &&
        meta_value->length() > 10) {
      auto obj = rockin::make_shared<MemObj>();
      obj->key = key;
      obj->type = META_TYPE(meta_value->c_str(), meta_value->length());
      obj->encode = META_ENCODE(meta_value->c_str(), meta_value->length());
      obj->version = META_VERSION(meta_value->c_str(), meta_value->length());
      obj->ttl = META_TTL(meta_value->c_str(), meta_value->length());
    }

    return nullptr;
  }

  const CmdInfo &info() { return info_; }

 private:
  CmdInfo info_;
};

}  // namespace rockin