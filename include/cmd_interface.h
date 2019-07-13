#pragma once
#include <iostream>
#include "coding.h"
#include "disk_saver.h"
#include "mem_db.h"
#include "utils.h"

// meta value header
// |   type   |  encode   |  version  |    ttl   |
// |   1 byte |   1 byte  |   4 byte  |  8 byte  |
#define BASE_META_VALUE_SIZE 14
#define META_VALUE_TYPE(base) DecodeFixed8((const char *)(base))
#define META_VALUE_ENCODE(base) DecodeFixed8((const char *)(base) + 1)
#define META_VALUE_VERSION(base) DecodeFixed32((const char *)(base) + 2)
#define META_VALUE_TTL(base) DecodeFixed64((const char *)(base) + 6)

#define SET_META_VALUE_HEADER(begin, type, encode, version, ttl) \
  do {                                                           \
    EncodeFixed8((char *)(begin), type);                         \
    EncodeFixed8((char *)(begin) + 1, encode);                   \
    EncodeFixed32((char *)(begin) + 2, version);                 \
    EncodeFixed64((char *)(begin) + 6, ttl);                     \
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