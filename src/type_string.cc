#include "type_string.h"
#include <glog/logging.h>
#include <math.h>
#include "cmd_args.h"
#include "coding.h"
#include "mem_alloc.h"
#include "mem_saver.h"
#include "rockin_conn.h"
#include "type_common.h"

// rocksdb save protocol
//
// meta key ->  key
//
// meta value-> |     meta value header     |   bulk   |
//              |     BASE_META_SIZE byte   |  2 byte  |
//
// data value-> |     data key header       |  bulk id |
//              |  BASE_DATA_KEY_SIZE byte  |  2 byte  |
//
// meta value->  split byte size
//

#define STRING_MAX_BULK_SIZE 1024
#define STRING_META_VALUE_SIZE 2
#define STRING_KEY_BULK_SIZE 2

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
  BufPtrs fkeys;
  for (int i = 0; i < bulk; i++) {
    int value_len = BASE_DATA_KEY_SIZE(mkey->len) + STRING_KEY_BULK_SIZE;
    auto fkey = make_buffer(value_len);
    SET_DATA_KEY_HEADER(fkey->data, mkey->data, mkey->len, version);
    EncodeFixed16(fkey->data + BASE_DATA_KEY_SIZE(mkey->len), i);

    fkeys.push_back(fkey);
  }

  return fkeys;
}

static inline BufPtrs GetStringFieldValues(BufPtr value) {
  BufPtrs values;
  if (value->len < STRING_MAX_BULK_SIZE) {
    values.push_back(value);
  } else {
    int bulk = STRING_BULK(value->len);
    for (int i = 0; i < bulk; i++) {
      auto new_value = make_buffer();
      new_value->data = value->data + (i * STRING_MAX_BULK_SIZE);
      new_value->len = (i == (bulk - 1) ? (value->len % STRING_MAX_BULK_SIZE)
                                        : STRING_MAX_BULK_SIZE);
      values.push_back(new_value);
    }
    values.push_back(value);
  }
  return values;
}

// key object  version type_err
typedef std::function<void(BufPtr, ObjPtr, uint64_t, bool)> GetObjCB;

static inline ObjPtr GetMetaResult(bool exist, BufPtr mkey,
                                   const std::string &meta, uint32_t &version,
                                   GetObjCB cb) {
  version = 0;
  if (!exist || meta.length() < BASE_META_VALUE_SIZE) {
    if (cb) cb(mkey, nullptr, 0, false);
    return nullptr;
  }

  version = META_VALUE_VERSION(meta.c_str());
  uint8_t type = META_VALUE_TYPE(meta.c_str());
  uint16_t expire = META_VALUE_EXPIRE(meta.c_str());
  if (type != Type_String ||
      meta.length() != BASE_META_VALUE_SIZE + STRING_META_VALUE_SIZE) {
    if (cb) cb(mkey, nullptr, version, type == Type_None ? false : true);
    return nullptr;
  }

  if (expire > 0 && GetMilliSec() >= expire) {
    if (cb) cb(mkey, nullptr, version, false);
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
                                     const std::vector<std::string> &values,
                                     GetObjCB cb) {
  if (exists.size() == 0 || values.size() != values.size()) {
    cb(obj->key, nullptr, obj->version, false);
    return nullptr;
  }
  size_t value_length = 0;
  for (size_t i = 0; i < exists.size(); i++) {
    if (!exists[i]) {
      cb(obj->key, nullptr, obj->version, false);
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

void GetStringObj(std::shared_ptr<RockinConn> conn, BufPtr key, GetObjCB cb) {
  // step1, get object from memory
  MemSaver::Default()->GetObj(
      conn->loop(), key, [conn, cb](BufPtr key, ObjPtr obj) {
        if (obj == nullptr) {
          // step2, get meta from rocksdb
          DiskSaver::Default()->GetMeta(
              conn->loop(), key,
              [conn, cb](bool exist, BufPtr mkey, const std::string &meta) {
                uint32_t version = 0;
                auto obj = GetMetaResult(exist, mkey, meta, version, cb);
                if (obj == nullptr) return;

                // step3, get field value form rocksdb
                auto field_keys = GetStringFieldKeys(
                    mkey, obj->version,
                    DecodeFixed16(meta.c_str() + BASE_META_VALUE_SIZE));
                DiskSaver::Default()->GetValues(
                    conn->loop(), mkey, field_keys,
                    [conn, obj, cb](BufPtr mkey,
                                    const std::vector<bool> &exists,
                                    const std::vector<std::string> &values) {
                      if (GetValuesResult(obj, exists, values, cb) == nullptr)
                        return;

                      // step4, insert into memory
                      MemSaver::Default()->InsertObj(
                          conn->loop(), obj, [cb](ObjPtr obj) {
                            cb(obj->key, obj, obj->version, false);
                          });
                    });
              });
        } else {
          if (obj->type != Type_String)
            cb(key, nullptr, obj->version, true);
          else
            cb(key, obj, obj->version, false);
        }
      });
}

ObjPtr UpdateStringObj(std::shared_ptr<RockinConn> conn, ObjPtr obj, BufPtr key,
                       BufPtr value, uint8_t encode, int32_t version,
                       uint64_t expire, bool update_meta,
                       std::function<void(ObjPtr)> cb) {
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

  auto async_num = std::make_shared<std::atomic<int>>(0);

  // step1, insert object to memory or update expire
  if (obj == nullptr) {
    async_num->fetch_add(1);
    MemSaver::Default()->InsertObj(conn->loop(), new_obj,
                                   [cb, async_num](ObjPtr obj) {
                                     async_num->fetch_sub(1);
                                     if (async_num->load() <= 0) cb(obj);
                                   });
  } else if (obj->expire != expire) {
  }

  uint16_t bulk = STRING_BULK(value->len);
  BufPtrs keys = GetStringFieldKeys(key, version, bulk);
  BufPtrs values = GetStringFieldValues(value);

  // step2, update object to rocksdb
  async_num->fetch_add(1);
  if (update_meta) {
    BufPtr meta = make_buffer(BASE_META_VALUE_SIZE + STRING_META_VALUE_SIZE);
    SET_META_VALUE_HEADER(meta->data, Type_String, encode, version, expire);
    EncodeFixed16(meta->data + BASE_META_VALUE_SIZE, bulk);

    DiskSaver::Default()->Set(conn->loop(), key, meta, keys, values,
                              [new_obj, cb, async_num](bool success) {
                                async_num->fetch_sub(1);
                                if (async_num->load() <= 0) cb(new_obj);
                              });
  } else {
    DiskSaver::Default()->Set(conn->loop(), key, keys, values,
                              [new_obj, cb, async_num](bool success) {
                                async_num->fetch_sub(1);
                                if (async_num->load() <= 0) cb(new_obj);
                              });
  }

  return new_obj;
}

#define OBJ_SET_NO_FLAGS 0
#define OBJ_SET_NX (1 << 0)
#define OBJ_SET_XX (1 << 1)
#define OBJ_SET_EX (1 << 2)
#define OBJ_SET_PX (1 << 3)

void SetStringForce(std::shared_ptr<RockinConn> conn, BufPtr key, BufPtr value,
                    int set_flags, uint64_t expire_ms,
                    std::function<void(ObjPtr)> cb) {
  // step1, get object from memory
  MemSaver::Default()->GetObj(
      conn->loop(), key,
      [conn, value, set_flags, expire_ms, cb](BufPtr key, ObjPtr obj) {
        if (obj == nullptr) {
          // step2, get meta from rocksdb
          DiskSaver::Default()->GetMeta(
              conn->loop(), key,
              [conn, value, set_flags, expire_ms, cb](bool exist, BufPtr mkey,
                                                      const std::string &meta) {
                uint32_t version = 0;
                auto obj = GetMetaResult(exist, mkey, meta, version, nullptr);
                if ((obj != nullptr && (set_flags & OBJ_SET_NX)) ||
                    (obj == nullptr && (set_flags & OBJ_SET_XX))) {
                  cb(nullptr);
                  return;
                }

                bool update_meta = false;
                if (obj == nullptr || obj->type != Type_String ||
                    obj->encode != Encode_Raw || obj->expire != expire_ms ||
                    DecodeFixed16(meta.c_str() + BASE_META_VALUE_SIZE) !=
                        STRING_BULK(value->len))
                  update_meta = true;

                // step3, udpate object to momery and rocksdb
                UpdateStringObj(conn, obj, mkey, value, Encode_Raw, version,
                                expire_ms, update_meta, cb);
              });
        } else {
          if (set_flags & OBJ_SET_NX) {
            cb(nullptr);
            return;
          }

          bool update_meta = false;
          if (obj->type != Type_String || obj->encode != Encode_Raw ||
              obj->expire != expire_ms ||
              STRING_BULK(OBJ_STRING(obj)->len) != STRING_BULK(value->len))
            update_meta = true;
          // step2, udpate object to momery and rocksdb
          UpdateStringObj(conn, obj, key, value, Encode_Raw, obj->version,
                          expire_ms, update_meta, cb);
        }
      });
}

///////////////////////////////////////////////////////////////////////////////
void GetCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                std::shared_ptr<RockinConn> conn) {
  auto &args = cmd_args->args();

  GetStringObj(conn, args[1],
               [conn](BufPtr key, ObjPtr obj, uint64_t version, bool type_err) {
                 if (type_err) {
                   conn->ReplyTypeError();
                 } else if (obj == nullptr) {
                   conn->ReplyNil();
                 } else {
                   conn->ReplyBulk(GenString(OBJ_STRING(obj), obj->encode));
                 }
               });
}

void SetCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                std::shared_ptr<RockinConn> conn) {
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
      conn->ReplySyntaxError();
      return;
    }
  }

  int64_t expire_time = 0;
  if (flags & (OBJ_SET_EX | OBJ_SET_PX)) {
    if (expire == nullptr) {
      conn->ReplySyntaxError();
      return;
    }
    if (StringToInt64(expire->data, expire->len, &expire_time) != 1 &&
        expire_time <= 0) {
      conn->ReplyError(g_set_time_err);
      return;
    }
  }

  if (expire_time > 0) {
    if (flags & OBJ_SET_EX) expire_time *= 1000;
    expire_time += GetMilliSec();
  }

  SetStringForce(conn, args[1], args[2], flags, expire_time,
                 [conn](ObjPtr obj) {
                   if (obj == nullptr)
                     conn->ReplyNil();
                   else
                     conn->ReplyOk();
                 });
}

void AppendCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                   std::shared_ptr<RockinConn> conn) {
  auto &args = cmd_args->args();

  GetStringObj(
      conn, args[1],
      [conn, value = args[2]](BufPtr key, ObjPtr obj, uint64_t version,
                              bool type_err) {
        if (type_err) {
          conn->ReplyTypeError();
          return;
        }

        BufPtr new_value = value;
        if (obj != nullptr) {
          auto str_value = GenString(OBJ_STRING(obj), obj->encode);
          size_t new_len = str_value->len + value->len;
          new_value = make_buffer(new_len, str_value);
          memcpy(new_value->data + str_value->len, value->data, value->len);
        }

        bool update_meta = false;
        if (obj == nullptr || obj->type != Type_String ||
            obj->encode != Encode_Raw ||
            STRING_BULK(OBJ_STRING(obj)->len) != STRING_BULK(new_value->len))
          update_meta = true;

        UpdateStringObj(conn, obj, key, new_value, Encode_Raw, version,
                        obj == nullptr ? 0 : obj->expire, update_meta,
                        [conn](ObjPtr obj) {
                          if (obj == nullptr)
                            conn->ReplyInteger(0);
                          else
                            conn->ReplyInteger(OBJ_STRING(obj)->len);
                        });
      });
}

void GetSetCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                   std::shared_ptr<RockinConn> conn) {
  auto &args = cmd_args->args();

  GetStringObj(
      conn, args[1],
      [conn, value = args[2]](BufPtr key, ObjPtr obj, uint64_t version,
                              bool type_err) {
        if (type_err) {
          conn->ReplyTypeError();
          return;
        }

        BufPtr old_value = obj == nullptr ? nullptr : OBJ_STRING(obj);
        int encode = obj == nullptr ? Encode_Raw : obj->encode;

        bool update_meta = false;
        if (obj == nullptr || obj->type != Type_String ||
            obj->encode != Encode_Raw ||
            STRING_BULK(OBJ_STRING(obj)->len) != STRING_BULK(value->len))
          update_meta = true;

        UpdateStringObj(conn, obj, key, value, Encode_Raw, version, 0,
                        update_meta, [conn, old_value, encode](ObjPtr obj) {
                          if (old_value == nullptr)
                            conn->ReplyNil();
                          else
                            conn->ReplyString(GenString(old_value, encode));
                        });
      });
}

void MGetCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                 std::shared_ptr<RockinConn> conn) {
  auto &args = cmd_args->args();
  auto rets = std::make_shared<MultiResult>(args.size() - 1);

  for (size_t i = 0; i < args.size() - 1; i++) {
    GetStringObj(conn, args[i + 1],
                 [conn, rets, i](BufPtr key, ObjPtr obj, uint64_t version,
                                 bool type_err) {
                   if (obj != nullptr && !type_err) {
                     rets->str_values[i] =
                         GenString(OBJ_STRING(obj), obj->encode);
                   }

                   rets->cnt.fetch_sub(1);
                   if (rets->cnt.load() == 0) {
                     conn->ReplyArray(rets->str_values);
                   }
                 });
  }
}

void MSetCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                 std::shared_ptr<RockinConn> conn) {
  auto &args = cmd_args->args();
  if (args.size() % 2 != 1) {
    static BufPtr g_reply_mset_args_err =
        make_buffer("ERR wrong number of arguments for MSET");
    conn->ReplyError(g_reply_mset_args_err);
    return;
  }

  int cnt = args.size() / 2;
  auto rets = std::make_shared<MultiResult>(cnt);
  auto async_num = std::make_shared<std::atomic<int>>(cnt);

  for (int i = 0; i < cnt; i++) {
    auto key = cmd_args->args()[i * 2 + 1];
    auto value = cmd_args->args()[i * 2 + 2];

    SetStringForce(conn, key, value, OBJ_SET_NO_FLAGS, 0,
                   [conn, async_num](ObjPtr obj) {
                     async_num->fetch_sub(1);
                     if (async_num->load() == 0) conn->ReplyOk();
                   });
  }
}

static void IncrDecrProcess(std::shared_ptr<RockinConn> conn, BufPtr key,
                            int num) {
  GetStringObj(
      conn, key,
      [conn, num](BufPtr key, ObjPtr obj, uint64_t version, bool type_err) {
        if (type_err) {
          conn->ReplyTypeError();
          return;
        }

        int64_t new_int = num;
        if (obj != nullptr) {
          int64_t oldv;
          if (!GenInt64(OBJ_STRING(obj), obj->encode, oldv)) {
            conn->ReplyIntegerError();
            return;
          }
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
        if (obj == nullptr || obj->type != Type_String ||
            obj->encode != Encode_Int)
          update_meta = true;

        UpdateStringObj(
            conn, obj, key, new_value, Encode_Int, version,
            obj == nullptr ? 0 : obj->expire, update_meta,
            [conn, new_int](ObjPtr obj) { conn->ReplyInteger(new_int); });
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
    conn->ReplyIntegerError();
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
    conn->ReplyIntegerError();
    return;
  }

  IncrDecrProcess(conn, args[1], -num);
}

static BufPtr g_reply_bit_err =
    make_buffer("bit offset is not an integer or out of range");

static bool GetBitOffset(BufPtr v, std::shared_ptr<RockinConn> conn,
                         int64_t &offset) {
  if (StringToInt64(v->data, v->len, &offset) != 1) {
    conn->ReplyError(g_reply_bit_err);
    return false;
  }

  if (offset < 0 || (offset >> 3) >= 512 * 1024 * 1024) {
    conn->ReplyError(g_reply_bit_err);
    return false;
  }

  return true;
}

BufPtr SetBitCmd::DoSetBit(BufPtr value, int64_t offset, int on, int &ret) {
  /*  int byte = offset >> 3;
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

   return value;*/
  return nullptr;
}

void SetBitCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                   std::shared_ptr<RockinConn> conn) {
  /* MemSaver::Default()->DoCmd(
      cmd_args->args()[1], [cmd_args, conn, cmd = shared_from_this()](
                               EventLoop *lt, std::shared_ptr<void> arg) {
        auto db = std::static_pointer_cast<MemDB>(arg);
        int64_t offset, on;
        auto &args = cmd_args->args();
        if (GetBitOffset(args[2], conn, offset) == false) {
          return;
        }

        if (StringToInt64(args[3]->data, args[3]->len, &on) != 1 || on & ~1) {
          conn->ReplyError(g_reply_bit_err);
          return;
        }

        uint32_t version = 0;
        bool type_err = false;
        auto obj = cmd->GetObj(conn->index(), db, args[1], type_err, version);
        if (type_err) {
          conn->ReplyTypeError();
          return;
        }

        int old_bit = 0;
        if (obj == nullptr) {
          cmd->AddObj(db, conn->index(), args[1],
                      cmd->DoSetBit(nullptr, offset, on, old_bit), Type_String,
                      Encode_Raw, version, 0);
        } else {
          cmd->UpdateObj(db, conn->index(), obj,
                         cmd->DoSetBit(GenString(OBJ_STRING(obj), obj->encode),
                                       offset, on, old_bit),
                         Type_String, Encode_Raw, obj->expire,
                         STRING_BULK(OBJ_STRING(obj)->len));
        }
        conn->ReplyInteger(old_bit);
      });*/
}

void GetBitCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                   std::shared_ptr<RockinConn> conn) {
  /*  MemSaver::Default()->DoCmd(
       cmd_args->args()[1], [cmd_args, conn, cmd = shared_from_this()](
                                EventLoop *lt, std::shared_ptr<void> arg) {
         auto db = std::static_pointer_cast<MemDB>(arg);
         int64_t offset;
         auto &args = cmd_args->args();
         if (GetBitOffset(args[2], conn, offset) == false) {
           return;
         }

         uint32_t version = 0;
         bool type_err = false;
         auto obj = cmd->GetObj(conn->index(), db, args[1], type_err, version);
         if (obj == nullptr) {
           if (type_err)
             conn->ReplyTypeError();
           else
             conn->ReplyInteger(0);
           return;
         }

         int byte = offset >> 3;
         auto str_value = GenString(OBJ_STRING(obj), obj->encode);
         if (str_value->len < byte + 1) {
           conn->ReplyInteger(0);
           return;
         }

         int bit = 7 - (offset & 0x7);
         char byteval = str_value->data[byte];
         conn->ReplyInteger((byteval & (1 << bit)) ? 1 : 0);
       });*/
}

void BitCountCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                     std::shared_ptr<RockinConn> conn) {
  /* MemSaver::Default()->DoCmd(
      cmd_args->args()[1], [cmd_args, conn, cmd = shared_from_this()](
                               EventLoop *lt, std::shared_ptr<void> arg) {
        auto db = std::static_pointer_cast<MemDB>(arg);
        auto &args = cmd_args->args();
        uint32_t version = 0;
        bool type_err = false;
        auto obj = cmd->GetObj(conn->index(), db, args[1], type_err, version);
        if (obj == nullptr) {
          if (type_err)
            conn->ReplyTypeError();
          else
            conn->ReplyInteger(0);
          return;
        }

        auto str_value = GenString(OBJ_STRING(obj), obj->encode);
        if (args.size() == 2) {
          conn->ReplyInteger(BitCount(str_value->data, str_value->len));
        } else if (args.size() == 4) {
          int64_t start, end;
          if (StringToInt64(args[2]->data, args[2]->len, &start) != 1 ||
              StringToInt64(args[3]->data, args[3]->len, &end) != 1) {
            conn->ReplyIntegerError();
            return;
          }

          if (start < 0 && end < 0 && start > end) {
            conn->ReplyInteger(0);
            return;
          }

          if (start < 0) start = str_value->len + start;
          if (end < 0) end = str_value->len + end;
          if (start < 0) start = 0;
          if (end < 0) end = 0;
          if (end >= str_value->len) end = str_value->len - 1;
          if (start > end) {
            conn->ReplyInteger(0);
          } else {
            conn->ReplyInteger(
                BitCount(str_value->data + start, end - start + 1));
          }
        } else {
          conn->ReplySyntaxError();
        }
      });*/
}

void BitopCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                  std::shared_ptr<RockinConn> conn) {
  /*  MemSaver::Default()->DoCmd(
       cmd_args->args()[2], [cmd_args, conn, cmd = shared_from_this()](
                                EventLoop *lt, std::shared_ptr<void> arg) {
 #define BITOP_AND 0
 #define BITOP_OR 1
 #define BITOP_XOR 2
 #define BITOP_NOT 3
         auto db = std::static_pointer_cast<MemDB>(arg);
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
           conn->ReplySyntaxError();
           return;
         }

         if (op == BITOP_NOT && args.size() != 4) {
           conn->ReplySyntaxError();
           return;
         }

         size_t maxlen = 0;
         std::vector<BufPtr> values;
         for (int i = 3; i < args.size(); i++) {
           uint32_t version = 0;
           bool type_err = false;
           auto obj = cmd->GetObj(conn->index(), db, args[i], type_err,
 version); if (type_err) { conn->ReplyTypeError(); return;
           }

           if (obj == nullptr) {
             values.push_back(nullptr);
           } else {
             auto str_value = GenString(OBJ_STRING(obj), obj->encode);
             if (str_value->len > maxlen) maxlen = str_value->len;
             values.push_back(str_value);
           }
         }

         if (maxlen > 0) {
           auto res = make_buffer(maxlen);
           for (int j = 0; j < maxlen; j++) {
             char output = (values[0] != nullptr && values[0]->len > j)
                               ? values[0]->data[j]
                               : 0;
             if (op == BITOP_NOT) output = ~output;
             for (int i = 1; i < values.size(); i++) {
               char byte = (values[i] != nullptr && values[i]->len > j)
                               ? values[i]->data[j]
                               : 0;
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
             res->data[j] = output;
           }

           // get from mem
           uint16_t old_bulk = 0;
           uint32_t version = 0;
           auto obj = db->Get(conn->index(), args[1]);
           if (obj == nullptr) {
             obj = cmd->GetMeta(conn->index(), args[1], old_bulk, version);
           }

           if (obj == nullptr) {
             obj = cmd->AddObj(db, conn->index(), args[1], args[2], Type_String,
                               Encode_Raw, version, 0);
           } else {
             if (obj->type == Type_String && obj->encode == Encode_Raw) {
               auto str_value = std::static_pointer_cast<buffer_t>(obj->value);
               old_bulk = STRING_BULK(str_value->len);
             }
             cmd->UpdateObj(db, conn->index(), obj, args[2], Type_String,
                            Encode_Raw, 0, old_bulk);
           }
         }

         conn->ReplyInteger(maxlen);
       });*/
}

void BitPosCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                   std::shared_ptr<RockinConn> conn) {
  /* MemSaver::Default()->DoCmd(
      cmd_args->args()[1], [cmd_args, conn, cmd = shared_from_this()](
                               EventLoop *lt, std::shared_ptr<void> arg) {
        auto db = std::static_pointer_cast<MemDB>(arg);
        auto &args = cmd_args->args();
        int64_t bit = 0;
        if (StringToInt64(args[2]->data, args[2]->len, &bit) != 1 || bit & ~1) {
          conn->ReplyIntegerError();
          return;
        }

        uint32_t version = 0;
        bool type_err = false;
        auto obj = cmd->GetObj(conn->index(), db, args[1], type_err, version);
        if (obj == nullptr) {
          if (type_err)
            conn->ReplyTypeError();
          else
            conn->ReplyInteger(bit ? -1 : 0);
          return;
        }

        auto str_value = GenString(OBJ_STRING(obj), obj->encode);
        bool end_given = false;
        int64_t start, end;
        if (args.size() == 4 || args.size() == 5) {
          if (StringToInt64(args[3]->data, args[3]->len, &start) != 1) {
            conn->ReplyIntegerError();
            return;
          }
          if (args.size() == 5) {
            if (StringToInt64(args[4]->data, args[4]->len, &end) != 1) {
              conn->ReplyIntegerError();
              return;
            }
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
          conn->ReplySyntaxError();
          return;
        }

        if (start > end) {
          conn->ReplyInteger(-1);
          return;
        }

        int bytes = end - start + 1;
        long pos = Bitpos(str_value->data + start, bytes, bit);

        if (end_given && bit == 0 && pos == bytes * 8) {
          conn->ReplyInteger(-1);
          return;
        }

        if (pos != -1) pos += start * 8;
        conn->ReplyInteger(pos);
      });*/
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
         auto obj = cmd->GetObj(conn->index(), db, args[1], type_err, version);
         if (obj == nullptr) {
           if (type_err)
             conn->ReplyTypeError();
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
