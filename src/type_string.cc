#include "type_string.h"
#include <glog/logging.h>
#include <math.h>
#include "cmd_args.h"
#include "cmd_reply.h"
#include "coding.h"
#include "mem_alloc.h"
#include "mem_saver.h"
#include "rockin_conn.h"
#include "type_control.h"
#include "workers.h"

// rocksdb save protocol
//
// meta key ->  key
//
// meta value-> |     meta value header     |   bulk   |
//              |     BASE_META_SIZE byte   |  2 byte  |
//
// data value-> |     data key header       |  bulk id |
//              |  BASE_FIELD_KEY_SIZE byte  |  2 byte  |
//
// meta value->  split byte size
//

#define STRING_MAX_BULK_SIZE 1024
#define STRING_META_VALUE_SIZE 2
#define STRING_FIELD_KEY_BULK_SIZE 2

#define STRING_FIELD_KEY_SIZE(len) \
  BASE_FIELD_KEY_SIZE(len) + STRING_FIELD_KEY_BULK_SIZE

#define OBJ_STRING(obj) std::static_pointer_cast<buffer_t>(obj->value)
#define BUF_INT64(v) (*((int64_t *)v->data))

#define STRING_BULK(len) \
  ((len) / STRING_MAX_BULK_SIZE + (((len) % STRING_MAX_BULK_SIZE) ? 1 : 0))

namespace rockin {

static inline BufPtr GenString(BufPtr value, int encode) {
  if (value != nullptr && encode == Encode_Int) {
    return make_buffer(Int64ToString(BUF_INT64(value)));
  }

  return value;
}

static inline bool GenInt64(BufPtr str, int encode, int64_t &v) {
  if (encode == Encode_Int) {
    v = BUF_INT64(str);
    return true;
  } else {
    if (StringToInt64(str->data, str->len, &v)) {
      return true;
    }
  }
  return false;
}

static inline BufPtrs GetStringFieldKeys(BufPtr mkey, uint32_t version,
                                         uint16_t bulk) {
  BufPtrs field_keys;
  for (int i = 0; i < bulk; i++) {
    auto field_key = make_buffer(STRING_FIELD_KEY_SIZE(mkey->len));
    SET_FIELD_KEY_HEADER(STRING_FLAG, field_key->data, mkey->data, mkey->len,
                         version);
    EncodeFixed16(field_key->data + BASE_FIELD_KEY_SIZE(mkey->len), i);
    field_keys.push_back(field_key);
  }

  return std::move(field_keys);
}

static inline KVPairS GetStringFieldKeyValues(BufPtr mkey, uint32_t version,
                                              BufPtr value) {
  KVPairS kvs;
  if (value->len < STRING_MAX_BULK_SIZE) {
    auto field_key = make_buffer(STRING_FIELD_KEY_SIZE(mkey->len));
    SET_FIELD_KEY_HEADER(STRING_FLAG, field_key->data, mkey->data, mkey->len,
                         version);
    EncodeFixed16(field_key->data + BASE_FIELD_KEY_SIZE(mkey->len), 0);
    kvs.push_back(std::make_pair(field_key, value));
  } else {
    int bulk = STRING_BULK(value->len);
    for (int i = 0; i < bulk; i++) {
      auto field_key = make_buffer(STRING_FIELD_KEY_SIZE(mkey->len));
      SET_FIELD_KEY_HEADER(STRING_FLAG, field_key->data, mkey->data, mkey->len,
                           version);
      EncodeFixed16(field_key->data + BASE_FIELD_KEY_SIZE(mkey->len), i);

      auto field_value = make_buffer();
      field_value->data = value->data + (i * STRING_MAX_BULK_SIZE);
      field_value->len = (i == (bulk - 1) ? (value->len % STRING_MAX_BULK_SIZE)
                                          : STRING_MAX_BULK_SIZE);

      kvs.push_back(std::make_pair(field_key, field_value));
    }
  }
  return std::move(kvs);
}

// key object  version type_err
static inline ObjPtr GetMetaResult(bool exist, BufPtr mkey,
                                   const std::string &meta, uint32_t &version,
                                   bool &type_err) {
  version = 0;
  type_err = false;
  if (!exist) return nullptr;

  if (meta.length() < BASE_META_VALUE_SIZE) {
    type_err = false;
    return nullptr;
  }

  version = META_VALUE_VERSION(meta.c_str());
  uint8_t type = META_VALUE_TYPE(meta.c_str());
  uint16_t expire = META_VALUE_EXPIRE(meta.c_str());

  if (type == Type_None || (expire > 0 && GetMilliSec() >= expire)) {
    return nullptr;
  }

  if (type != Type_String ||
      meta.length() != BASE_META_VALUE_SIZE + STRING_META_VALUE_SIZE) {
    type_err = true;
    return nullptr;
  }

  auto obj = make_object(mkey);
  obj->type = type;
  obj->encode = META_VALUE_ENCODE(meta.c_str());
  obj->version = version;
  obj->expire = expire;
  return obj;
}

static inline ObjPtr GetValuesResult(ObjPtr obj,
                                     const std::vector<bool> &exists,
                                     const std::vector<std::string> &values) {
  if (exists.size() == 0 || values.size() != values.size()) {
    return nullptr;
  }

  size_t value_length = 0;
  for (size_t i = 0; i < exists.size(); i++) {
    if (!exists[i]) {
      return nullptr;
    }
    value_length += values[i].length();
  }

  size_t offset = 0;
  auto value = make_buffer(value_length);
  for (size_t i = 0; i < values.size(); i++) {
    memcpy(value->data + offset, values[i].c_str(), values[i].length());
    offset += values[i].length();
  }
  obj->value = value;
  return obj;
}

ObjPtr GetStringObj(BufPtr key, uint32_t &version, bool type_err) {
  version = 0;
  type_err = false;

  // step1, get object from memory
  auto obj = MemSaver::Default()->GetObj(key);
  if (obj == nullptr) {
    bool exist = false;
    // step2, get meta from rocksdb
    std::string meta = DiskSaver::Default()->GetMeta(key, exist);
    obj = GetMetaResult(exist, key, meta, version, type_err);
    if (obj == nullptr) return nullptr;

    // step3, get field value form rocksdb
    std::vector<bool> exists;
    auto field_keys = GetStringFieldKeys(
        key, obj->version, DecodeFixed16(meta.c_str() + BASE_META_VALUE_SIZE));
    auto values = DiskSaver::Default()->GetValues(key, field_keys, exists);

    obj = GetValuesResult(obj, exists, values);
    if (obj == nullptr) return nullptr;

    // step4, insert into memory
    MemSaver::Default()->InsertObj(obj);
  } else {
    version = obj->version;
    if (obj->type != Type_String) {
      type_err = true;
      return nullptr;
    }
  }

  return obj;
}

ObjPtr UpdateStringObj(ObjPtr obj, BufPtr key, BufPtr value, uint8_t encode,
                       uint32_t version, uint64_t expire, bool update_meta) {
  if (update_meta) version++;
  auto new_obj = obj;
  if (new_obj == nullptr) {
    new_obj = make_object(key);
    new_obj->expire = expire;
  }
  new_obj->type = Type_String;
  new_obj->encode = encode;
  new_obj->version = version;
  new_obj->value = value;

  // step1, insert object to memory or update expire
  if (obj == nullptr) {
    MemSaver::Default()->InsertObj(new_obj);
  } else if (obj->expire != expire) {
    MemSaver::Default()->UpdateExpire(new_obj, expire);
  }

  uint16_t bulk = STRING_BULK(value->len);
  KVPairS kvs = GetStringFieldKeyValues(key, version, value);

  // step2, update object to rocksdb
  if (update_meta) {
    BufPtr meta = make_buffer(BASE_META_VALUE_SIZE + STRING_META_VALUE_SIZE);
    SET_META_VALUE_HEADER(meta->data, Type_String, encode, version, expire);
    EncodeFixed16(meta->data + BASE_META_VALUE_SIZE, bulk);

    DiskSaver::Default()->Set(key, meta, kvs);
  } else {
    DiskSaver::Default()->Set(key, kvs);
  }

  return new_obj;
}

#define OBJ_SET_NO_FLAGS 0
#define OBJ_SET_NX (1 << 0)
#define OBJ_SET_XX (1 << 1)
#define OBJ_SET_EX (1 << 2)
#define OBJ_SET_PX (1 << 3)

bool SetStringForce(BufPtr key, BufPtr value, int set_flags,
                    uint64_t expire_ms) {
  // step1, get object from memory
  bool bulk = 0;
  uint32_t version = 0;
  auto obj = MemSaver::Default()->GetObj(key);
  if (obj == nullptr) {
    // step2, get object meta from rocksdb
    bool exist = false, type_err = false;
    std::string meta = DiskSaver::Default()->GetMeta(key, exist);
    obj = GetMetaResult(exist, key, meta, version, type_err);
    if (obj != nullptr)
      bulk = DecodeFixed16(meta.c_str() + BASE_META_VALUE_SIZE);
  }

  if ((obj != nullptr && (set_flags & OBJ_SET_NX)) ||
      (obj == nullptr && (set_flags & OBJ_SET_XX))) {
    return false;
  }

  if (obj) {
    version = obj->version;
    if (obj->value != nullptr && obj->type == Type_String)
      bulk = STRING_BULK(OBJ_STRING(obj)->len);
  }

  bool update_meta = false;
  if (obj == nullptr || obj->type != Type_String || obj->encode != Encode_Raw ||
      obj->expire != expire_ms || bulk != STRING_BULK(value->len))
    update_meta = true;

  // step3, udpate object to momery and rocksdb
  UpdateStringObj(obj, key, value, Encode_Raw, version, expire_ms, update_meta);
  return true;
}

///////////////////////////////////////////////////////////////////////////////
void GetCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                std::shared_ptr<RockinConn> conn) {
  Workers::Default()->AsyncWork(cmd_args->args()[1], conn, [cmd_args]() {
    uint32_t version = 0;
    bool type_err = false;
    auto &args = cmd_args->args();
    auto obj = GetStringObj(args[1], version, type_err);
    if (type_err)
      return ReplyTypeError();
    else if (obj == nullptr)
      return ReplyNil();
    else
      return ReplyString(GenString(OBJ_STRING(obj), obj->encode));
  });
}

void SetCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                std::shared_ptr<RockinConn> conn) {
  Workers::Default()->AsyncWork(cmd_args->args()[1], conn, [cmd_args]() {
    auto &args = cmd_args->args();
    static auto g_set_time_err = make_buffer("ERR invalid expire time in set");

    BufPtr expire = nullptr;
    int flags = OBJ_SET_NO_FLAGS;
    for (int j = 3; j < args.size(); j++) {
      BufPtr a = args[j];
      BufPtr next = (j == args.size() - 1) ? nullptr : args[j + 1];

      if (a->len == 2 && (a->data[0] == 'n' || a->data[0] == 'N') &&
          (a->data[1] == 'x' || a->data[1] == 'X') && !(flags & OBJ_SET_XX)) {
        flags |= OBJ_SET_NX;
      } else if (a->len == 2 && (a->data[0] == 'x' || a->data[0] == 'X') &&
                 (a->data[1] == 'x' || a->data[1] == 'X') &&
                 !(flags & OBJ_SET_NX)) {
        flags |= OBJ_SET_XX;
      } else if (a->len == 2 && (a->data[0] == 'e' || a->data[0] == 'E') &&
                 (a->data[1] == 'x' || a->data[1] == 'X') &&
                 !(flags & OBJ_SET_PX) && next != nullptr) {
        flags |= OBJ_SET_EX;
        expire = next;
        j++;
      } else if (a->len == 2 && (a->data[0] == 'p' || a->data[0] == 'P') &&
                 (a->data[1] == 'x' || a->data[1] == 'X') &&
                 !(flags & OBJ_SET_EX) && next != nullptr) {
        flags |= OBJ_SET_PX;
        expire = next;
        j++;
      } else {
        return ReplySyntaxError();
      }
    }

    int64_t expire_time = 0;
    if (flags & (OBJ_SET_EX | OBJ_SET_PX)) {
      if (expire == nullptr) return ReplySyntaxError();

      if (StringToInt64(expire->data, expire->len, &expire_time) != 1 &&
          expire_time <= 0)
        return ReplyError(g_set_time_err);
    }

    if (expire_time > 0) {
      if (flags & OBJ_SET_EX) expire_time *= 1000;
      expire_time += GetMilliSec();
    }

    bool ret = SetStringForce(args[1], args[2], flags, expire_time);
    if (ret)
      return ReplyOk();
    else
      return ReplyNil();
  });
}

void AppendCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                   std::shared_ptr<RockinConn> conn) {
  Workers::Default()->AsyncWork(cmd_args->args()[1], conn, [cmd_args]() {
    uint32_t version = 0;
    bool type_err = false;
    auto &args = cmd_args->args();
    auto obj = GetStringObj(args[1], version, type_err);
    if (type_err) return ReplyTypeError();

    BufPtr new_value = args[2];
    if (obj != nullptr) {
      auto str_value = GenString(OBJ_STRING(obj), obj->encode);
      size_t new_len = str_value->len + args[2]->len;
      new_value = make_buffer(new_len, str_value);
      memcpy(new_value->data + str_value->len, args[2]->data, args[2]->len);
    }

    bool update_meta = false;
    if (obj == nullptr || obj->type != Type_String ||
        obj->encode != Encode_Raw ||
        STRING_BULK(OBJ_STRING(obj)->len) != STRING_BULK(new_value->len))
      update_meta = true;

    obj = UpdateStringObj(obj, args[1], new_value, Encode_Raw, version,
                          obj == nullptr ? 0 : obj->expire, update_meta);

    return ReplyInteger(new_value->len);
  });
}

void GetSetCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                   std::shared_ptr<RockinConn> conn) {
  Workers::Default()->AsyncWork(cmd_args->args()[1], conn, [cmd_args]() {
    uint32_t version = 0;
    bool type_err = false;
    auto &args = cmd_args->args();
    auto obj = GetStringObj(args[1], version, type_err);
    if (type_err) return ReplyTypeError();

    BufPtr old_value = nullptr;
    if (obj != nullptr) old_value = GenString(OBJ_STRING(obj), obj->encode);

    bool update_meta = false;
    if (obj == nullptr || obj->type != Type_String ||
        obj->encode != Encode_Raw ||
        STRING_BULK(OBJ_STRING(obj)->len) != STRING_BULK(args[2]->len))
      update_meta = true;

    UpdateStringObj(obj, args[1], args[2], Encode_Raw, version, 0, update_meta);

    return ReplyString(old_value);
  });
}

void MGetCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                 std::shared_ptr<RockinConn> conn) {
  auto &args = cmd_args->args();
  auto rets = std::make_shared<MultiResult>(args.size() - 1);

  for (size_t i = 0; i < args.size() - 1; i++) {
    Workers::Default()->AsyncWork(
        cmd_args->args()[1], conn, [rets, key = args[i + 1], i]() {
          uint32_t version = 0;
          bool type_err = false;
          auto obj = GetStringObj(key, version, type_err);
          if (obj != nullptr)
            rets->str_values[i] = GenString(OBJ_STRING(obj), obj->encode);

          rets->cnt.fetch_sub(1);
          if (rets->cnt.load() == 0) {
            return ReplyArray(rets->str_values);
          }
          return BufPtrs();
        });
  }
}

void MSetCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                 std::shared_ptr<RockinConn> conn) {
  auto &args = cmd_args->args();
  if (args.size() % 2 != 1) {
    static BufPtr g_reply_mset_args_err =
        make_buffer("ERR wrong number of arguments for MSET");
    conn->WriteData(ReplyError(g_reply_mset_args_err));
    return;
  }

  int cnt = args.size() / 2;
  auto async_num = std::make_shared<std::atomic<int>>(cnt);

  for (int i = 0; i < cnt; i++) {
    Workers::Default()->AsyncWork(
        cmd_args->args()[1], conn,
        [async_num, key = args[i * 2 + 1], value = args[i * 2 + 2]]() {
          SetStringForce(key, value, OBJ_SET_NO_FLAGS, 0);
          async_num->fetch_sub(1);
          if (async_num->load() == 0)
            return ReplyOk();
          else
            return BufPtrs();
        });
  }
}

static void IncrDecrProcess(std::shared_ptr<RockinConn> conn, BufPtr key,
                            int num) {
  Workers::Default()->AsyncWork(key, conn, [key, num]() {
    uint32_t version = 0;
    bool type_err = false;
    auto obj = GetStringObj(key, version, type_err);
    if (type_err) return ReplyTypeError();

    int64_t new_int = num;
    if (obj != nullptr) {
      int64_t oldv;
      if (!GenInt64(OBJ_STRING(obj), obj->encode, oldv))
        return ReplyIntegerError();
      new_int += oldv;
    }

    BufPtr new_value = nullptr;
    if (OBJ_STRING(obj)->len == sizeof(int64_t)) {
      new_value = OBJ_STRING(obj);
    } else {
      new_value = make_buffer(sizeof(int64_t));
    }
    BUF_INT64(new_value) = new_int;

    bool update_meta = false;
    if (obj == nullptr || obj->type != Type_String || obj->encode != Encode_Int)
      update_meta = true;

    std::cout << "update_meta:" << update_meta << std::endl;
    UpdateStringObj(obj, key, new_value, Encode_Int, version,
                    obj == nullptr ? 0 : obj->expire, update_meta);

    return ReplyInteger(new_int);
  });
}

void IncrCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                 std::shared_ptr<RockinConn> conn) {
  auto &args = cmd_args->args();
  IncrDecrProcess(conn, args[1], 1);
}
void IncrbyCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                   std::shared_ptr<RockinConn> conn) {
  int64_t num;
  auto &args = cmd_args->args();
  if (StringToInt64(args[2]->data, args[2]->len, &num) == 0) {
    conn->WriteData(ReplyIntegerError());
    return;
  }

  IncrDecrProcess(conn, args[1], num);
}

void DecrCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                 std::shared_ptr<RockinConn> conn) {
  auto &args = cmd_args->args();
  IncrDecrProcess(conn, args[1], -1);
}

void DecrbyCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                   std::shared_ptr<RockinConn> conn) {
  int64_t num;
  auto &args = cmd_args->args();
  if (StringToInt64(args[2]->data, args[2]->len, &num) == 0) {
    conn->WriteData(ReplyIntegerError());
    return;
  }

  IncrDecrProcess(conn, args[1], -num);
}

static BufPtr g_reply_bit_err =
    make_buffer("bit offset is not an integer or out of range");

static bool GetBitOffset(BufPtr v, int64_t &offset) {
  if (StringToInt64(v->data, v->len, &offset) != 1) {
    return false;
  }

  if (offset < 0 || (offset >> 3) >= 512 * 1024 * 1024) {
    return false;
  }

  return true;
}

static BufPtr DoSetBit(BufPtr value, int64_t offset, int on, int &ret) {
  int byte = offset >> 3;
  if (value == nullptr) {
    value = make_buffer(byte + 1, value);
    memset(value->data, 0, value->len);
  } else if (byte + 1 > value->len) {
    int oldlen = value->len;
    value = make_buffer(byte + 1, value);
    memset(value->data + oldlen, 0, value->len - oldlen);
  }

  int bit = 7 - (offset & 0x7);
  char byteval = value->data[byte];
  ret = ((byteval & (1 << bit)) ? 1 : 0);

  byteval &= ~(1 << bit);
  byteval |= ((on & 0x1) << bit);
  value->data[byte] = byteval;

  return value;
}

void SetBitCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                   std::shared_ptr<RockinConn> conn) {
  Workers::Default()->AsyncWork(cmd_args->args()[1], conn, [cmd_args]() {
    int64_t offset, on;
    auto &args = cmd_args->args();

    if (GetBitOffset(args[2], offset) == false) {
      return ReplyError(g_reply_bit_err);
    }

    if (StringToInt64(args[3]->data, args[3]->len, &on) != 1 || on & ~1) {
      return ReplyError(g_reply_bit_err);
    }

    uint32_t version = 0;
    bool type_err = false;
    auto obj = GetStringObj(args[1], version, type_err);
    if (type_err) return ReplyTypeError();

    int ret = 0;
    auto value = DoSetBit(
        obj == nullptr ? nullptr : GenString(OBJ_STRING(obj), obj->encode),
        offset, on, ret);

    bool update_meta = false;
    if (obj == nullptr || obj->type != Type_String ||
        obj->encode != Encode_Raw ||
        STRING_BULK(OBJ_STRING(obj)->len) != STRING_BULK(value->len))
      update_meta = true;

    UpdateStringObj(obj, args[1], value, Encode_Raw, version,
                    obj != nullptr ? obj->expire : 0, update_meta);

    return ReplyInteger(ret);
  });
}

void GetBitCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                   std::shared_ptr<RockinConn> conn) {
  Workers::Default()->AsyncWork(cmd_args->args()[1], conn, [cmd_args]() {
    int64_t offset, on;
    auto &args = cmd_args->args();

    if (GetBitOffset(args[2], offset) == false) {
      return ReplyError(g_reply_bit_err);
    }

    uint32_t version = 0;
    bool type_err = false;
    auto obj = GetStringObj(args[1], version, type_err);
    if (type_err) return ReplyTypeError();
    if (obj == nullptr) return ReplyInteger(0);

    int byte = offset >> 3;
    auto str_value = GenString(OBJ_STRING(obj), obj->encode);
    if (str_value->len < byte + 1) return ReplyInteger(0);

    int bit = 7 - (offset & 0x7);
    char byteval = str_value->data[byte];
    return ReplyInteger((byteval & (1 << bit)) ? 1 : 0);
  });
}

void BitCountCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                     std::shared_ptr<RockinConn> conn) {
  Workers::Default()->AsyncWork(cmd_args->args()[1], conn, [cmd_args]() {
    uint32_t version = 0;
    bool type_err = false;
    auto &args = cmd_args->args();
    auto obj = GetStringObj(args[1], version, type_err);
    if (type_err) return ReplyTypeError();
    if (obj == nullptr) return ReplyInteger(0);

    auto str_value = GenString(OBJ_STRING(obj), obj->encode);
    if (args.size() == 2) {
      return ReplyInteger(BitCount(str_value->data, str_value->len));
    } else if (args.size() == 4) {
      int64_t start, end;
      if (StringToInt64(args[2]->data, args[2]->len, &start) != 1 ||
          StringToInt64(args[3]->data, args[3]->len, &end) != 1) {
        return ReplyIntegerError();
      }

      if (start < 0 && end < 0 && start > end) return ReplyInteger(0);

      if (start < 0) start = str_value->len + start;
      if (end < 0) end = str_value->len + end;
      if (start < 0) start = 0;
      if (end < 0) end = 0;
      if (end >= str_value->len) end = str_value->len - 1;
      if (start > end)
        return ReplyInteger(0);
      else
        return ReplyInteger(BitCount(str_value->data + start, end - start + 1));
    } else {
      return ReplySyntaxError();
    }
  });
}

struct BitOpHelper {
  std::atomic<bool> error;
  std::atomic<int> count;
  std::vector<BufPtr> values;

  BitOpHelper(int cnt) : count(cnt), error(false), values(cnt) {}
};

void BitopCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                  std::shared_ptr<RockinConn> conn) {
#define BITOP_AND 0
#define BITOP_OR 1
#define BITOP_XOR 2
#define BITOP_NOT 3
  auto &args = cmd_args->args();

  int op = 0;
  if (args[1]->len == 3 &&
      (args[1]->data[0] == 'a' || args[1]->data[0] == 'A') &&
      (args[1]->data[1] == 'n' || args[1]->data[1] == 'N') &&
      (args[1]->data[2] == 'd' || args[1]->data[2] == 'D')) {
    op = BITOP_AND;
  } else if (args[1]->len == 2 &&
             (args[1]->data[0] == 'o' || args[1]->data[0] == 'O') &&
             (args[1]->data[1] == 'r' || args[1]->data[1] == 'R')) {
    op = BITOP_OR;
  } else if (args[1]->len == 3 &&
             (args[1]->data[0] == 'x' || args[1]->data[0] == 'X') &&
             (args[1]->data[1] == 'o' || args[1]->data[1] == 'O') &&
             (args[1]->data[2] == 'r' || args[1]->data[2] == 'R')) {
    op = BITOP_XOR;
  } else if (args[1]->len == 3 &&
             (args[1]->data[0] == 'n' || args[1]->data[0] == 'N') &&
             (args[1]->data[1] == 'o' || args[1]->data[1] == 'O') &&
             (args[1]->data[2] == 't' || args[1]->data[3] == 'T')) {
    op = BITOP_NOT;
  } else {
    conn->WriteData(ReplySyntaxError());
    return;
  }

  if (op == BITOP_NOT && args.size() != 4) {
    conn->WriteData(ReplySyntaxError());
    return;
  }

  BufPtrs mkeys;
  for (size_t i = 3; i < args.size(); i++) mkeys.push_back(args[i]);

  auto type_err_flag = std::make_shared<std::atomic<bool>>(false);
  Workers::Default()->AsyncWork(
      mkeys, conn,
      [type_err_flag](BufPtr key) {
        uint32_t version = 0;
        bool type_err = false;
        auto obj = GetStringObj(key, version, type_err);
        if (type_err) type_err_flag->store(true);
        return obj;
      },
      args[2],
      [key = args[2], type_err_flag, op](const ObjPtrs &objs) {
        if (type_err_flag->load()) return ReplyTypeError();

        uint32_t version = 0;
        bool type_err = false;
        auto obj = GetStringObj(key, version, type_err);
        if (type_err) return ReplyTypeError();

        int max_len = 0;
        for (size_t i = 0; i < objs.size(); i++) {
          if (objs[i] != nullptr) {
            int len = OBJ_STRING(objs[i])->len;
            if (len > max_len) max_len = len;
          }
        }

        if (max_len > 0) {
          BufPtr new_value = nullptr;
          if (obj != nullptr && OBJ_STRING(obj)->len == max_len)
            new_value = OBJ_STRING(obj);
          else
            new_value = make_buffer(max_len);

          for (int j = 0; j < max_len; j++) {
            char output = 0;
            if (objs[0] != nullptr && OBJ_STRING(objs[0])->len > j)
              output = OBJ_STRING(objs[0])->data[j];

            if (op == BITOP_NOT) output = ~output;
            for (int i = 1; i < objs.size(); i++) {
              char byte = 0;
              if (objs[i] != nullptr && OBJ_STRING(objs[i])->len > j)
                byte = OBJ_STRING(objs[i])->data[j];

              switch (op) {
                case BITOP_AND:
                  output &= byte;
                  break;
                case BITOP_OR:
                  output |= byte;
                  break;
                case BITOP_XOR:
                  output ^= byte;
                  break;
              }
            }
            new_value->data[j] = output;
          }

          bool update_meta = false;
          if (obj == nullptr || obj->type != Type_String ||
              obj->encode != Encode_Raw || obj->expire != 0 ||
              STRING_BULK(OBJ_STRING(obj)->len) != STRING_BULK(max_len))
            update_meta = true;
          UpdateStringObj(obj, key, new_value, Encode_Raw, version, 0,
                          update_meta);
        }

        return ReplyInteger(max_len);
      });
}

void BitPosCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                   std::shared_ptr<RockinConn> conn) {
  Workers::Default()->AsyncWork(cmd_args->args()[1], conn, [cmd_args]() {
    int64_t bit = 0;
    auto &args = cmd_args->args();
    if (StringToInt64(args[2]->data, args[2]->len, &bit) != 1 || bit & ~1)
      return ReplyIntegerError();

    uint32_t version = 0;
    bool type_err = false;
    auto obj = GetStringObj(args[1], version, type_err);
    if (type_err)
      return ReplyTypeError();
    else if (obj == nullptr)
      return ReplyInteger(bit ? -1 : 0);

    auto str_value = GenString(OBJ_STRING(obj), obj->encode);
    bool end_given = false;
    int64_t start, end;
    if (args.size() == 4 || args.size() == 5) {
      if (StringToInt64(args[3]->data, args[3]->len, &start) != 1)
        return ReplyIntegerError();
      if (args.size() == 5) {
        if (StringToInt64(args[4]->data, args[4]->len, &end) != 1)
          return ReplyIntegerError();
        end_given = true;
      } else {
        end = str_value->len - 1;
      }
      if (start < 0) start = str_value->len + start;
      if (end < 0) end = str_value->len + end;
      if (start < 0) start = 0;
      if (end < 0) end = 0;
      if (end >= str_value->len) end = str_value->len - 1;
    } else if (args.size() == 3) {
      start = 0;
      end = str_value->len - 1;
    } else {
      return ReplySyntaxError();
    }

    if (start > end) return ReplyInteger(-1);

    int bytes = end - start + 1;
    long pos = Bitpos(str_value->data + start, bytes, bit);
    if (end_given && bit == 0 && pos == bytes * 8) return ReplyInteger(-1);

    if (pos != -1) pos += start * 8;
    return ReplyInteger(pos);
  });
}

void StringDebug::Do(std::shared_ptr<CmdArgs> cmd_args,
                     std::shared_ptr<RockinConn> conn) {
  /*  MemSaver::Default()->DoCmd(
       cmd_args->args()[1], [cmd_args, conn, cmd = shared_from_this()](
                                EventLoop *lt, std::shared_ptr<void> arg) {
         auto db = std::static_pointer_cast<MemDB>(arg);

         uint32_t version = 0;
         bool type_err = false;
         auto &args = cmd_args->args();
         auto obj = cmd->GetObj(conn->index(), db, args[1], type_err,
     version); if (obj == nullptr) { if (type_err) conn->ReplyTypeError();
           else
             conn->ReplyNil();
           return;
         }

         std::vector<BufPtr> values;
         values.push_back(
             make_buffer(Format("type:%d", obj->type)));
         values.push_back(
             make_buffer(Format("encode:%d", obj->encode)));
         values.push_back(
             make_buffer(Format("version:%u", obj->version)));
         values.push_back(
             make_buffer(Format("expire:%llu", obj->expire)));
         std::string key(obj->key->data, obj->key->len);
         values.push_back(make_buffer(
             Format("key[%d]:%s", key.length(), key.c_str())));
         std::string value(OBJ_STRING(obj)->data, OBJ_STRING(obj)->len);
         values.push_back(make_buffer(
             Format("value[%d]:%s", value.length(), value.c_str())));

         conn->ReplyArray(values);
       });*/
}

}  // namespace rockin
