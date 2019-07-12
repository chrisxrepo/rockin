#include "type_string.h"
#include <glog/logging.h>
#include <math.h>
#include "cmd_args.h"
#include "coding.h"
#include "mem_db.h"
#include "mem_saver.h"
#include "rockin_conn.h"
#include "type_common.h"

namespace rockin {

#define STRING_MAX_BULK_SIZE 1024

#define STRING_META_VALUE_SIZE 2
#define STRING_KEY_BULK_SIZE 2

#define STRING_BULK(len) \
  ((len) / STRING_MAX_BULK_SIZE + (((len) % STRING_MAX_BULK_SIZE) ? 1 : 0))

#define CHECK_STRING_META(o, t, e, ob, nb) \
  ((o) != nullptr && (o)->type == (t) && (o)->encode == (e) && (ob) == (nb))

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
bool StringCmd::Update(int dbindex, std::shared_ptr<MemObj> obj,
                       bool update_meta) {
  auto str_value = OBJ_STRING(obj);
  size_t bulk = STRING_BULK(str_value->len);
  if (update_meta) obj->version++;

  KVPairS kvs;
  for (size_t i = 0; i < bulk; i++) {
    auto key = rockin::make_shared<membuf_t>(BASE_DATA_KEY_SIZE(obj->key->len) +
                                             STRING_KEY_BULK_SIZE);

    SET_DATA_KEY_HEADER(key->data, obj->key->data, obj->key->len, obj->version);
    EncodeFixed16(key->data + BASE_DATA_KEY_SIZE(obj->key->len), i);

    auto value = rockin::make_shared<membuf_t>();
    value->data = str_value->data + (i * STRING_MAX_BULK_SIZE);
    value->len = (i == (bulk - 1) ? (str_value->len % STRING_MAX_BULK_SIZE)
                                  : STRING_MAX_BULK_SIZE);

    kvs.push_back(std::make_pair(key, value));
  }

  auto diskdb = DiskSaver::Default()->GetDB(obj->key);
  if (update_meta) {
    MemPtr meta = rockin::make_shared<membuf_t>(BASE_META_VALUE_SIZE +
                                                STRING_META_VALUE_SIZE);
    auto str_value = OBJ_STRING(obj);
    SET_META_VALUE_HEADER(meta->data, obj->type, obj->encode, obj->version,
                          obj->ttl);
    EncodeFixed16(meta->data + BASE_META_VALUE_SIZE,
                  STRING_BULK(str_value->len));

    diskdb->SetMetaDatas(dbindex, obj->key, meta, kvs);
  } else {
    diskdb->SetDatas(dbindex, kvs);
  }

  return true;
}

std::shared_ptr<MemObj> StringCmd::GetMeta(int dbindex, MemPtr key,
                                           uint16_t &bulk) {
  std::string meta;
  auto diskdb = DiskSaver::Default()->GetDB(key);
  if (diskdb->GetMeta(dbindex, key, &meta) &&
      meta.length() >= BASE_META_VALUE_SIZE) {
    auto obj = rockin::make_shared<MemObj>();
    obj->key = key;
    obj->type = META_VALUE_TYPE(meta.c_str());
    obj->encode = META_VALUE_ENCODE(meta.c_str());
    obj->version = META_VALUE_VERSION(meta.c_str());
    obj->ttl = META_VALUE_TTL(meta.c_str());

    if (obj->type == Type_String &&
        meta.length() == BASE_META_VALUE_SIZE + STRING_META_VALUE_SIZE)
      bulk = DecodeFixed16(meta.c_str() + BASE_META_VALUE_SIZE);
    else
      bulk = 0;

    return obj;
  }

  return nullptr;
}

std::shared_ptr<MemObj> StringCmd::GetObj(int dbindex,
                                          std::shared_ptr<MemDB> db, MemPtr key,
                                          bool &type_err, uint32_t &version) {
  version = 0;
  type_err = false;
  auto obj = db->Get(dbindex, key);
  if (obj != nullptr) {
    if (obj->type != Type_String) {
      type_err = true;
      return nullptr;
    }
    version = obj->version;
    return obj;
  }

  uint16_t bulk = 0;
  obj = GetMeta(dbindex, key, bulk);
  if (obj == nullptr) {
    return nullptr;
  }

  if (obj->type != Type_String) {
    type_err = true;
    return nullptr;
  }

  version = obj->version;
  if (obj->type == Type_None) {
    return nullptr;
  }

  std::vector<MemPtr> keys;
  std::vector<std::string> values;
  for (int i = 0; i < bulk; i++) {
    int value_len = BASE_DATA_KEY_SIZE(key->len) + STRING_KEY_BULK_SIZE;
    auto nkey = rockin::make_shared<membuf_t>(value_len);
    SET_DATA_KEY_HEADER(nkey->data, key->data, key->len, version);
    EncodeFixed16(nkey->data + BASE_DATA_KEY_SIZE(key->len), i);

    keys.push_back(nkey);
  }

  auto diskdb = DiskSaver::Default()->GetDB(key);
  auto rets = diskdb->GetDatas(dbindex, keys, &values);

  LOG_IF(FATAL, rets.size() != keys.size()) << "GetDatas rets.size!=keys.size";
  if (values.size() != rets.size()) {
    return nullptr;
  }

  size_t value_length = 0;
  for (size_t i = 0; i < rets.size(); i++) {
    if (!rets[i]) {
      return nullptr;
    }
    value_length += values[i].length();
  }

  size_t offset = 0;
  auto value = rockin::make_shared<membuf_t>(value_length);
  for (size_t i = 0; i < values.size(); i++) {
    memcpy(value->data + offset, values[i].c_str(), values[i].length());
    offset += values[i].length();
  }
  obj->value = value;
  db->Insert(dbindex, obj);

  return obj;
}

std::shared_ptr<MemObj> StringCmd::AddObj(std::shared_ptr<MemDB> db,
                                          int dbindex, MemPtr key, MemPtr value,
                                          int type, int encode,
                                          uint32_t version) {
  auto obj = rockin::make_shared<MemObj>();
  obj->type = type;
  obj->encode = encode;
  obj->version = version;
  obj->key = key;
  obj->value = value;
  db->Insert(dbindex, obj);
  this->Update(dbindex, obj, true);
  return obj;
}

std::shared_ptr<MemObj> StringCmd::UpdateObj(int dbindex,
                                             std::shared_ptr<MemObj> obj,
                                             MemPtr value, int type, int encode,
                                             int old_bulk) {
  bool update_meta = false;
  if (old_bulk == 0 && obj->value != nullptr) {
    update_meta = !CHECK_STRING_META(obj, type, encode,
                                     (STRING_BULK(OBJ_STRING(obj)->len)),
                                     STRING_BULK(value->len));
  } else {
    update_meta = !CHECK_STRING_META(obj, type, encode, old_bulk,
                                     STRING_BULK(value->len));
  }
  OBJ_SET_VALUE(obj, value, type, encode);
  Update(dbindex, obj, update_meta);
  return obj;
}

///////////////////////////////////////////////////////////////////////////////
void GetCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                std::shared_ptr<RockinConn> conn) {
  MemSaver::Default()->DoCmd(
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
        conn->ReplyBulk(GenString(OBJ_STRING(obj), obj->encode));
      });
}

void SetCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                std::shared_ptr<RockinConn> conn) {
  MemSaver::Default()->DoCmd(
      cmd_args->args()[1], [cmd_args, conn, cmd = shared_from_this()](
                               EventLoop *lt, std::shared_ptr<void> arg) {
        auto db = std::static_pointer_cast<MemDB>(arg);
        uint16_t old_bulk = 0;
        auto &args = cmd_args->args();
        auto obj = db->Get(conn->index(), args[1]);
        if (obj != nullptr) {
          if (obj->type == Type_String && obj->encode == Encode_Raw) {
            auto str_value = std::static_pointer_cast<membuf_t>(obj->value);
            old_bulk = STRING_BULK(str_value->len);
          }
        } else {
          obj = cmd->GetMeta(conn->index(), args[1], old_bulk);
          if (obj == nullptr) {
            obj = rockin::make_shared<MemObj>();
            obj->key = args[1];
          }
          db->Insert(conn->index(), obj);
        }

        cmd->UpdateObj(conn->index(), obj, args[2], Type_String, Encode_Raw,
                       old_bulk);
        conn->ReplyOk();
      });
}

void AppendCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                   std::shared_ptr<RockinConn> conn) {
  MemSaver::Default()->DoCmd(
      cmd_args->args()[1], [cmd_args, conn, cmd = shared_from_this()](
                               EventLoop *lt, std::shared_ptr<void> arg) {
        auto db = std::static_pointer_cast<MemDB>(arg);

        uint32_t version = 0;
        bool type_err = false;
        auto &args = cmd_args->args();
        auto obj = cmd->GetObj(conn->index(), db, args[1], type_err, version);
        if (type_err) {
          conn->ReplyTypeError();
          return;
        }

        MemPtr str_value;
        if (obj == nullptr) {
          str_value = args[2];
          obj = cmd->AddObj(db, conn->index(), args[1], args[2], Type_String,
                            Encode_Raw, version);
        } else if (obj->type == Type_String) {
          auto tmp_value = GenString(OBJ_STRING(obj), obj->encode);
          str_value = rockin::make_shared<membuf_t>(
              tmp_value->len + args[2]->len, tmp_value);
          memcpy(str_value->data + tmp_value->len, args[2]->data, args[2]->len);

          cmd->UpdateObj(conn->index(), obj, str_value, Type_String, Encode_Raw,
                         0);
        }

        conn->ReplyInteger(str_value->len);
      });
}

void GetSetCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                   std::shared_ptr<RockinConn> conn) {
  MemSaver::Default()->DoCmd(
      cmd_args->args()[1], [cmd_args, conn, cmd = shared_from_this()](
                               EventLoop *lt, std::shared_ptr<void> arg) {
        auto db = std::static_pointer_cast<MemDB>(arg);

        uint32_t version = 0;
        bool type_err = false;
        auto &args = cmd_args->args();
        auto obj = cmd->GetObj(conn->index(), db, args[1], type_err, version);
        if (type_err) {
          conn->ReplyTypeError();
          return;
        }
        conn->ReplyObj(obj);

        if (obj == nullptr) {
          cmd->AddObj(db, conn->index(), args[1], args[2], Type_String,
                      Encode_Raw, version);
        } else {
          cmd->UpdateObj(conn->index(), obj, args[2], Type_String, Encode_Raw,
                         0);
        }
      });
}

void MGetCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                 std::shared_ptr<RockinConn> conn) {
  int cnt = cmd_args->args().size() - 1;
  auto rets = std::make_shared<MultiResult>(cnt);

  for (int i = 0; i < cnt; i++) {
    MemSaver::Default()->DoCmd(
        cmd_args->args()[i + 1],
        [rets, i, cmd_args, conn, cmd = shared_from_this()](
            EventLoop *lt, std::shared_ptr<void> arg) {
          auto db = std::static_pointer_cast<MemDB>(arg);

          uint32_t version = 0;
          bool type_err = false;
          auto &args = cmd_args->args();
          auto obj =
              cmd->GetObj(conn->index(), db, args[i + 1], type_err, version);
          if (obj != nullptr) {
            rets->str_values[i] = GenString(OBJ_STRING(obj), obj->encode);
          }

          rets->cnt--;
          if (rets->cnt.load() == 0) {
            conn->ReplyArray(rets->str_values);
          }
        });
  }
}

void MSetCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                 std::shared_ptr<RockinConn> conn) {
  if (cmd_args->args().size() % 2 != 1) {
    static MemPtr g_reply_mset_args_err =
        rockin::make_shared<membuf_t>("ERR wrong number of arguments for MSET");

    conn->ReplyError(g_reply_mset_args_err);
    return;
  }

  int cnt = cmd_args->args().size() / 2;
  auto rets = std::make_shared<MultiResult>(cnt);

  for (int i = 0; i < cnt; i++) {
    MemSaver::Default()->DoCmd(
        cmd_args->args()[i * 2 + 1],
        [rets, i, cmd_args, conn, cmd = shared_from_this()](
            EventLoop *lt, std::shared_ptr<void> arg) {
          auto db = std::static_pointer_cast<MemDB>(arg);
          uint16_t old_bulk = 0;
          auto &args = cmd_args->args();
          auto obj = db->Get(conn->index(), args[i * 2 + 1]);
          if (obj != nullptr) {
            if (obj->type == Type_String && obj->encode == Encode_Raw) {
              auto str_value = std::static_pointer_cast<membuf_t>(obj->value);
              old_bulk = STRING_BULK(str_value->len);
            }
          } else {
            obj = cmd->GetMeta(conn->index(), args[i * 2 + 1], old_bulk);
            if (obj == nullptr) {
              obj = rockin::make_shared<MemObj>();
              obj->key = args[i * 2 + 1];
            }
            db->Insert(conn->index(), obj);
          }
          cmd->UpdateObj(conn->index(), obj, args[i * 2 + 2], Type_String,
                         Encode_Raw, old_bulk);
          rets->cnt--;
          if (rets->cnt.load() == 0) {
            conn->ReplyOk();
          }
        });
  }
}

static void IncrDecrProcess(std::shared_ptr<StringCmd> cmd,
                            std::shared_ptr<CmdArgs> cmd_args,
                            std::shared_ptr<RockinConn> conn,
                            std::shared_ptr<MemDB> db, int num) {
  uint32_t version = 0;
  bool type_err = false;
  auto &args = cmd_args->args();
  auto obj = cmd->GetObj(conn->index(), db, args[1], type_err, version);
  if (type_err) {
    conn->ReplyTypeError();
    return;
  }

  if (obj == nullptr) {
    auto value = rockin::make_shared<membuf_t>(sizeof(int64_t));
    BUF_INT64(value) = num;
    cmd->AddObj(db, conn->index(), args[1], value, Type_String, Encode_Int,
                version);
  } else {
    int64_t oldv;
    if (!GenInt64(OBJ_STRING(obj), obj->encode, oldv)) {
      conn->ReplyIntegerError();
      return;
    }

    MemPtr new_value = nullptr;
    if (obj->encode == Encode_Int) {
      new_value = OBJ_STRING(obj);
      BUF_INT64(new_value) = oldv + num;
    } else {
      new_value = rockin::make_shared<membuf_t>(sizeof(int64_t));
      BUF_INT64(new_value) = oldv + num;
    }
    cmd->UpdateObj(conn->index(), obj, new_value, Type_String, Encode_Int, 0);
  }
  conn->ReplyObj(obj);
}

void IncrCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                 std::shared_ptr<RockinConn> conn) {
  MemSaver::Default()->DoCmd(cmd_args->args()[1],
                             [cmd_args, conn, cmd = shared_from_this()](
                                 EventLoop *lt, std::shared_ptr<void> arg) {
                               auto db = std::static_pointer_cast<MemDB>(arg);
                               IncrDecrProcess(cmd, cmd_args, conn, db, 1);
                             });
}
void IncrbyCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                   std::shared_ptr<RockinConn> conn) {
  MemSaver::Default()->DoCmd(
      cmd_args->args()[1], [cmd_args, conn, cmd = shared_from_this()](
                               EventLoop *lt, std::shared_ptr<void> arg) {
        auto db = std::static_pointer_cast<MemDB>(arg);

        int64_t tmpv;
        auto &args = cmd_args->args();
        if (StringToInt64(args[2]->data, args[2]->len, &tmpv) == 0) {
          conn->ReplyIntegerError();
          return;
        }

        IncrDecrProcess(cmd, cmd_args, conn, db, tmpv);
      });
}

void DecrCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                 std::shared_ptr<RockinConn> conn) {
  MemSaver::Default()->DoCmd(cmd_args->args()[1],
                             [cmd_args, conn, cmd = shared_from_this()](
                                 EventLoop *lt, std::shared_ptr<void> arg) {
                               auto db = std::static_pointer_cast<MemDB>(arg);
                               IncrDecrProcess(cmd, cmd_args, conn, db, -1);
                             });
}

void DecrbyCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                   std::shared_ptr<RockinConn> conn) {
  MemSaver::Default()->DoCmd(
      cmd_args->args()[1], [cmd_args, conn, cmd = shared_from_this()](
                               EventLoop *lt, std::shared_ptr<void> arg) {
        auto db = std::static_pointer_cast<MemDB>(arg);

        int64_t tmpv;
        auto &args = cmd_args->args();
        if (StringToInt64(args[2]->data, args[2]->len, &tmpv) == 0) {
          conn->ReplyIntegerError();
          return;
        }

        IncrDecrProcess(cmd, cmd_args, conn, db, -tmpv);
      });
}

static MemPtr g_reply_bit_err = rockin::make_shared<membuf_t>(
    "bit offset is not an integer or out of range");

static bool GetBitOffset(MemPtr v, std::shared_ptr<RockinConn> conn,
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

MemPtr SetBitCmd::DoSetBit(MemPtr value, int64_t offset, int on, int &ret) {
  int byte = offset >> 3;
  if (value == nullptr) {
    value = rockin::make_shared<membuf_t>(byte + 1, value);
    memset(value->data, 0, value->len);
  } else if (byte + 1 > value->len) {
    int oldlen = value->len;
    value = rockin::make_shared<membuf_t>(byte + 1, value);
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
  MemSaver::Default()->DoCmd(
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
                      Encode_Raw, version);
        } else {
          int old_bulk = STRING_BULK(OBJ_STRING(obj)->len);
          auto value = GenString(OBJ_STRING(obj), obj->encode);
          cmd->UpdateObj(conn->index(), obj,
                         cmd->DoSetBit(value, offset, on, old_bit), Type_String,
                         Encode_Raw, old_bulk);
        }
        conn->ReplyInteger(old_bit);
      });
}

void GetBitCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                   std::shared_ptr<RockinConn> conn) {
  MemSaver::Default()->DoCmd(
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
      });
}

void BitCountCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                     std::shared_ptr<RockinConn> conn) {
  MemSaver::Default()->DoCmd(
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
      });
}

void BitopCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                  std::shared_ptr<RockinConn> conn) {
  MemSaver::Default()->DoCmd(
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
        std::vector<MemPtr> values;
        for (int i = 3; i < args.size(); i++) {
          uint32_t version = 0;
          bool type_err = false;
          auto obj = cmd->GetObj(conn->index(), db, args[i], type_err, version);
          if (type_err) {
            conn->ReplyTypeError();
            return;
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
          auto res = rockin::make_shared<membuf_t>(maxlen);
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

          uint16_t old_bulk;
          auto obj = db->Get(conn->index(), args[2]);
          if (obj != nullptr) {
            if (obj->type == Type_String && obj->encode == Encode_Raw) {
              auto str_value = std::static_pointer_cast<membuf_t>(obj->value);
              old_bulk = STRING_BULK(str_value->len);
            }
          } else {
            obj = cmd->GetMeta(conn->index(), args[2], old_bulk);
            if (obj == nullptr) {
              obj = rockin::make_shared<MemObj>();
              obj->key = args[2];
            }
            db->Insert(conn->index(), obj);
          }

          cmd->UpdateObj(conn->index(), obj, res, Type_String, Encode_Raw,
                         old_bulk);
        }

        conn->ReplyInteger(maxlen);
      });
}

void BitPosCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                   std::shared_ptr<RockinConn> conn) {
  MemSaver::Default()->DoCmd(
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

        if (pos != -1) pos += start * 8; /* Adjust for the bytes we skipped. */
        conn->ReplyInteger(pos);
      });
}

}  // namespace rockin
