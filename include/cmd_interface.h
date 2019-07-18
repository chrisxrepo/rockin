#pragma once
#include <iostream>
#include "coding.h"
#include "disk_saver.h"
#include "utils.h"

// meta value header
// |   type   |  encode   |  version  |    expire   |
// |   1 byte |   1 byte  |   4 byte  |  8 byte  |
#define BASE_META_VALUE_SIZE 14
#define META_VALUE_TYPE(base) DecodeFixed8((const char *)(base))
#define META_VALUE_ENCODE(base) DecodeFixed8((const char *)(base) + 1)
#define META_VALUE_VERSION(base) DecodeFixed32((const char *)(base) + 2)
#define META_VALUE_EXPIRE(base) DecodeFixed64((const char *)(base) + 6)

#define SET_META_TYPE(begin, t) EncodeFixed8((char *)(begin), (t))
#define SET_META_ENCODE(begin, e) EncodeFixed8((char *)(begin) + 1, (e))
#define SET_META_VERSION(begin, v) EncodeFixed32((char *)(begin) + 2, (v))
#define SET_META_EXPIRE(begin, ex) EncodeFixed64((char *)(begin) + 6, (ex))

#define SET_META_VALUE_HEADER(begin, type, encode, version, expire) \
  do {                                                              \
    SET_META_TYPE(begin, type);                                     \
    SET_META_ENCODE(begin, encode);                                 \
    SET_META_VERSION(begin, version);                               \
    SET_META_EXPIRE(begin, expire);                                 \
  } while (0)

// data key header
// |  key len  |   key    |  version  |
// |   2 byte  |   n byte |   4 byte  |
#define BASE_DATA_KEY_SIZE(n) (6 + (n))
#define DATA_KEY_LNE(begin) DecodeFixed16((const char *)(begin))
#define DATA_KEY_START(begin) ((const char *)(begin) + 2)
#define DATA_KEY_VERSION(begin) \
  DecodeFixed32((const char *)(begin) + (DATA_KEY_LNE(begin) + 2))

#define SET_DATA_KEY_HEADER(begin, key, len, version)    \
  do {                                                   \
    EncodeFixed16((char *)(begin), len);                 \
    memcpy((char *)(begin) + 2, key, len);               \
    EncodeFixed32((char *)(begin) + (len + 2), version); \
  } while (0)

#define CHECK_META(o, t, e, ex)                                \
  ((o) != nullptr && (o)->type == (t) && (o)->encode == (e) && \
   (o)->expire == (ex))

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

  const CmdInfo &info() { return info_; }

  std::shared_ptr<object_t> GetMeta(int dbindex, BufPtr key, std::string &meta,
                                    uint32_t &version) {
    version = 0;
    auto diskdb = DiskSaver::Default()->GetDB(key);
    bool exist = diskdb->GetMeta(key, &meta);
    if (exist && meta.length() >= BASE_META_VALUE_SIZE) {
      version = META_VALUE_VERSION(meta.c_str());
      uint8_t type = META_VALUE_TYPE(meta.c_str());
      uint64_t expire = META_VALUE_EXPIRE(meta.c_str());
      if (type == Type_None || (expire > 0 && expire <= GetMilliSec())) {
        return nullptr;
      }

      auto obj = make_object();
      obj->key = key;
      obj->type = type;
      obj->encode = META_VALUE_ENCODE(meta.c_str());
      obj->version = version;
      obj->expire = expire;
      return obj;
    }
    return nullptr;
  }

 private:
  CmdInfo info_;
};

}  // namespace rockin