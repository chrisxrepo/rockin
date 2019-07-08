#include "type_string.h"
#include <glog/logging.h>
#include <math.h>
#include "cmd_args.h"
#include "mem_db.h"
#include "mem_saver.h"
#include "rockin_conn.h"
#include "type_common.h"

namespace rockin {
std::string StringCmd::EncodeKey(std::shared_ptr<buffer_t> key) {}

std::shared_ptr<buffer_t> StringCmd::DecodeKey(const std::string &key) {}

void GetCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                std::shared_ptr<RockinConn> conn) {
  MemSaver::Default()->DoCmd(
      cmd_args->args()[1],
      [cmd_args, conn](EventLoop *lt, std::shared_ptr<void> arg) {
        auto db = std::static_pointer_cast<MemDB>(arg);
        auto obj = db->GetReplyNil(conn->index(), cmd_args->args()[1], conn);
        if (obj == nullptr || !CheckAndReply(obj, conn, Type_String)) {
          return;
        }

        conn->ReplyBulk(GenString(OBJ_STRING(obj), obj->encode));
      });
}

void SetCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                std::shared_ptr<RockinConn> conn) {
  MemSaver::Default()->DoCmd(
      cmd_args->args()[1],
      [cmd_args, conn](EventLoop *lt, std::shared_ptr<void> arg) {
        auto db = std::static_pointer_cast<MemDB>(arg);
        auto &args = cmd_args->args();
        db->Set(conn->index(), args[1], args[2], Type_String, Encode_Raw);
        conn->ReplyOk();
      });
}

void AppendCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                   std::shared_ptr<RockinConn> conn) {
  MemSaver::Default()->DoCmd(
      cmd_args->args()[1],
      [cmd_args, conn](EventLoop *lt, std::shared_ptr<void> arg) {
        auto db = std::static_pointer_cast<MemDB>(arg);
        auto &args = cmd_args->args();
        auto obj = db->Get(conn->index(), args[1]);
        if (obj != nullptr && !CheckAndReply(obj, conn, Type_String)) {
          return;
        }

        std::shared_ptr<buffer_t> str_value;
        if (obj == nullptr) {
          str_value = args[2];
          db->Set(conn->index(), args[1], str_value, Type_String, Encode_Raw);
        } else if (obj->type == Type_String) {
          str_value = GenString(OBJ_STRING(obj), obj->encode);
          int oldlen = str_value->len;
          str_value = copy_buffer(str_value, oldlen + args[2]->len);
          memcpy(str_value->data + oldlen, args[2]->data, args[2]->len);
          OBJ_SET_VALUE(obj, str_value, Type_String, Encode_Raw);
        }

        conn->ReplyInteger(str_value->len);
      });
}

void GetSetCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                   std::shared_ptr<RockinConn> conn) {
  MemSaver::Default()->DoCmd(
      cmd_args->args()[1],
      [cmd_args, conn](EventLoop *lt, std::shared_ptr<void> arg) {
        auto db = std::static_pointer_cast<MemDB>(arg);
        auto &args = cmd_args->args();
        auto obj = db->Get(conn->index(), args[1]);
        if (obj != nullptr && !CheckAndReply(obj, conn, Type_String)) {
          return;
        }
        ReplyRedisObj(obj, conn);

        if (obj == nullptr) {
          db->Set(conn->index(), args[1], args[2], Type_String, Encode_Raw);
        } else {
          obj->value = args[2];
          obj->type = Type_String;
          obj->encode = Encode_Raw;
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
        [cmd_args, conn, rets, i](EventLoop *lt, std::shared_ptr<void> arg) {
          auto &args = cmd_args->args();
          auto db = std::static_pointer_cast<MemDB>(arg);
          auto obj = db->Get(conn->index(), args[i + 1]);
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
    static std::shared_ptr<buffer_t> g_reply_mset_args_err =
        make_buffer("ERR wrong number of arguments for MSET");

    conn->ReplyError(g_reply_mset_args_err);
    return;
  }

  int cnt = cmd_args->args().size() / 2;
  auto rets = std::make_shared<MultiResult>(cnt);

  for (int i = 0; i < cnt; i++) {
    MemSaver::Default()->DoCmd(
        cmd_args->args()[i * 2 + 1],
        [cmd_args, conn, rets, i](EventLoop *lt, std::shared_ptr<void> arg) {
          auto &args = cmd_args->args();
          auto db = std::static_pointer_cast<MemDB>(arg);
          db->Set(conn->index(), args[i * 2 + 1], args[i * 2 + 2], Type_String,
                  Encode_Raw);

          rets->cnt--;
          if (rets->cnt.load() == 0) {
            conn->ReplyOk();
          }
        });
  }
}

static void IncrDecrProcess(std::shared_ptr<CmdArgs> cmd_args,
                            std::shared_ptr<RockinConn> conn,
                            std::shared_ptr<MemDB> db, int num) {
  auto &args = cmd_args->args();
  auto obj = db->Get(conn->index(), args[1]);
  if (obj != nullptr && !CheckAndReply(obj, conn, Type_String)) {
    return;
  }

  if (obj == nullptr) {
    auto value = make_buffer(sizeof(int64_t));
    BUF_INT64(value) = num;
    obj = db->Set(conn->index(), args[1], value, Type_String, Encode_Int);
  } else {
    int64_t oldv;
    if (!GenInt64(OBJ_STRING(obj), obj->encode, oldv)) {
      conn->ReplyIntegerError();
      return;
    }

    if (obj->encode == Encode_Int) {
      BUF_INT64(OBJ_STRING(obj)) = oldv + num;
    } else {
      auto value = make_buffer(sizeof(int64_t));
      BUF_INT64(value) = oldv + num;
      OBJ_SET_VALUE(obj, value, Type_String, Encode_Int);
    }
  }
  ReplyRedisObj(obj, conn);
}

void IncrCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                 std::shared_ptr<RockinConn> conn) {
  MemSaver::Default()->DoCmd(
      cmd_args->args()[1],
      [cmd_args, conn](EventLoop *lt, std::shared_ptr<void> arg) {
        auto db = std::static_pointer_cast<MemDB>(arg);
        IncrDecrProcess(cmd_args, conn, db, 1);
      });
}
void IncrbyCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                   std::shared_ptr<RockinConn> conn) {
  MemSaver::Default()->DoCmd(
      cmd_args->args()[1],
      [cmd_args, conn](EventLoop *lt, std::shared_ptr<void> arg) {
        auto db = std::static_pointer_cast<MemDB>(arg);

        int64_t tmpv;
        auto &args = cmd_args->args();
        if (StringToInt64(args[2]->data, args[2]->len, &tmpv) == 0) {
          conn->ReplyIntegerError();
          return;
        }

        IncrDecrProcess(cmd_args, conn, db, tmpv);
      });
}

void DecrCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                 std::shared_ptr<RockinConn> conn) {
  MemSaver::Default()->DoCmd(
      cmd_args->args()[1],
      [cmd_args, conn](EventLoop *lt, std::shared_ptr<void> arg) {
        auto db = std::static_pointer_cast<MemDB>(arg);
        IncrDecrProcess(cmd_args, conn, db, -1);
      });
}

void DecrbyCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                   std::shared_ptr<RockinConn> conn) {
  MemSaver::Default()->DoCmd(
      cmd_args->args()[1],
      [cmd_args, conn](EventLoop *lt, std::shared_ptr<void> arg) {
        auto db = std::static_pointer_cast<MemDB>(arg);

        int64_t tmpv;
        auto &args = cmd_args->args();
        if (StringToInt64(args[2]->data, args[2]->len, &tmpv) == 0) {
          conn->ReplyIntegerError();
          return;
        }

        IncrDecrProcess(cmd_args, conn, db, -tmpv);
      });
}

static std::shared_ptr<buffer_t> g_reply_bit_err =
    make_buffer("bit offset is not an integer or out of range");

static bool GetBitOffset(std::shared_ptr<buffer_t> v,
                         std::shared_ptr<RockinConn> conn, int64_t &offset) {
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

void SetBitCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                   std::shared_ptr<RockinConn> conn) {
  MemSaver::Default()->DoCmd(
      cmd_args->args()[1],
      [cmd_args, conn](EventLoop *lt, std::shared_ptr<void> arg) {
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

        auto obj = db->Get(conn->index(), args[1]);
        if (obj != nullptr && !CheckAndReply(obj, conn, Type_String)) {
          return;
        }

        int byte = offset >> 3;
        std::shared_ptr<buffer_t> str_value;
        if (obj == nullptr) {
          str_value = make_buffer(byte + 1);
          obj = db->Set(conn->index(), args[1], str_value, Type_String,
                        Encode_Raw);
        } else {
          str_value = GenString(OBJ_STRING(obj), obj->encode);
          OBJ_SET_VALUE(obj, str_value, Type_String, Encode_Raw);
        }

        if (byte + 1 > str_value->len) {
          int oldlen = str_value->len;
          str_value = copy_buffer(str_value, byte + 1);
          memset(str_value->data + oldlen, 0, str_value->len - oldlen);
          OBJ_SET_VALUE(obj, str_value, Type_String, Encode_Raw);
        }

        int bit = 7 - (offset & 0x7);
        char byteval = str_value->data[byte];
        conn->ReplyInteger((byteval & (1 << bit)) ? 1 : 0);

        byteval &= ~(1 << bit);
        byteval |= ((on & 0x1) << bit);
        str_value->data[byte] = byteval;
      });
}

void GetBitCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                   std::shared_ptr<RockinConn> conn) {
  MemSaver::Default()->DoCmd(
      cmd_args->args()[1],
      [cmd_args, conn](EventLoop *lt, std::shared_ptr<void> arg) {
        auto db = std::static_pointer_cast<MemDB>(arg);
        int64_t offset;
        auto &args = cmd_args->args();
        if (GetBitOffset(args[2], conn, offset) == false) {
          return;
        }

        auto obj = db->Get(conn->index(), args[1]);
        if (obj == nullptr) {
          conn->ReplyInteger(0);
          return;
        }
        if (!CheckAndReply(obj, conn, Type_String)) {
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
      cmd_args->args()[1],
      [cmd_args, conn](EventLoop *lt, std::shared_ptr<void> arg) {
        auto db = std::static_pointer_cast<MemDB>(arg);
        auto &args = cmd_args->args();
        auto obj = db->Get(conn->index(), args[1]);
        if (obj == nullptr) {
          conn->ReplyInteger(0);
          return;
        }
        if (!CheckAndReply(obj, conn, Type_String)) {
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
      cmd_args->args()[1],
      [cmd_args, conn](EventLoop *lt, std::shared_ptr<void> arg) {
#define BITOP_AND 0
#define BITOP_OR 1
#define BITOP_XOR 2
#define BITOP_NOT 3
        auto db = std::static_pointer_cast<MemDB>(arg);
        auto &args = cmd_args->args();
        int op = 0;
        if (args[2]->len == 3 &&
            (args[2]->data[0] == 'a' || args[2]->data[0] == 'A') &&
            (args[2]->data[1] == 'n' || args[2]->data[1] == 'N') &&
            (args[2]->data[2] == 'd' || args[2]->data[2] == 'D')) {
          op = BITOP_AND;
        } else if (args[2]->len == 2 &&
                   (args[2]->data[0] == 'o' || args[2]->data[0] == 'O') &&
                   (args[2]->data[1] == 'r' || args[2]->data[1] == 'R')) {
          op = BITOP_OR;
        } else if (args[2]->len == 3 &&
                   (args[2]->data[0] == 'x' || args[2]->data[0] == 'X') &&
                   (args[2]->data[1] == 'o' || args[2]->data[1] == 'O') &&
                   (args[2]->data[2] == 'r' || args[2]->data[2] == 'R')) {
          op = BITOP_XOR;
        } else if (args[2]->len == 3 &&
                   (args[2]->data[0] == 'n' || args[2]->data[0] == 'N') &&
                   (args[2]->data[1] == 'o' || args[2]->data[1] == 'O') &&
                   (args[2]->data[2] == 't' || args[2]->data[3] == 'T')) {
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
        std::vector<std::shared_ptr<buffer_t>> values;
        for (int i = 3; i < args.size(); i++) {
          auto obj = db->Get(conn->index(), args[i]);
          if (obj == nullptr) {
            values.push_back(nullptr);
          } else {
            if (!CheckAndReply(obj, conn, Type_String)) {
              return;
            }
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

          db->Set(conn->index(), args[1], res, Type_String, Encode_Raw);
        }

        conn->ReplyInteger(maxlen);
      });
}

void BitPosCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                   std::shared_ptr<RockinConn> conn) {
  MemSaver::Default()->DoCmd(
      cmd_args->args()[1],
      [cmd_args, conn](EventLoop *lt, std::shared_ptr<void> arg) {
        auto db = std::static_pointer_cast<MemDB>(arg);
        auto &args = cmd_args->args();
        int64_t bit = 0;
        if (StringToInt64(args[2]->data, args[2]->len, &bit) != 1 || bit & ~1) {
          conn->ReplyIntegerError();
          return;
        }

        auto obj = db->Get(conn->index(), args[1]);
        if (obj == nullptr) {
          conn->ReplyInteger(bit ? -1 : 0);
          return;
        }
        if (!CheckAndReply(obj, conn, Type_String)) {
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
