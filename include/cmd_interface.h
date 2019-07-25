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
// | key type |  key len  |   key    |  version  |
// |  1 byte  |   2 byte  |   n byte |   4 byte  |

#define STRING_FLAG 'S'

#define BASE_FIELD_KEY_SIZE(n) (7 + (n))
#define FIELD_KEY_LNE(begin) DecodeFixed16((const char *)(begin) + 1)
#define FIELD_KEY_START(begin) ((const char *)(begin) + 3)
#define FIELD_KEY_VERSION(begin) \
  DecodeFixed32((const char *)(begin) + (FIELD_KEY_LNE(begin) + 3))

#define SET_FIELD_KEY_HEADER(type, begin, key, len, version) \
  do {                                                       \
    EncodeFixed8((char *)(begin), type);                     \
    EncodeFixed16((char *)(begin) + 1, len);                 \
    memcpy((char *)(begin) + 3, key, len);                   \
    EncodeFixed32((char *)(begin) + (len + 3), version);     \
  } while (0)

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

 private:
  CmdInfo info_;
};

}  // namespace rockin