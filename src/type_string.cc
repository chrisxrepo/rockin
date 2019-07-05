#include "type_string.h"
#include <glog/logging.h>
#include <math.h>
#include "cmd_args.h"
#include "mem_db.h"
#include "mem_saver.h"
#include "type_common.h"

namespace rockin {
// get key
void GetCommand(std::shared_ptr<CmdArgs> cmd) {
  std::pair<EventLoop *, MemDB *> loop =
      MemSaver::Default()->GetDB(cmd->args()[1]);

  auto db = loop.second;
  loop.first->RunInLoopNoWait(
      [cmd, db](EventLoop *et, std::shared_ptr<void> arg) {
        auto obj = db->GetReplyNil(cmd->DbIndex(), cmd->args()[1], cmd);
        if (obj == nullptr || !CheckAndReply(obj, cmd, Type_String)) {
          return;
        }

        cmd->ReplyBulk(GenString(OBJ_STRING(obj), obj->encode));
      },
      nullptr);
}

// set key value
void SetCommand(std::shared_ptr<CmdArgs> cmd) {
  std::pair<EventLoop *, MemDB *> loop =
      MemSaver::Default()->GetDB(cmd->args()[1]);

  auto db = loop.second;
  loop.first->RunInLoopNoWait(
      [cmd, db](EventLoop *et, std::shared_ptr<void> arg) {
        auto &args = cmd->args();
        db->Set(cmd->DbIndex(), args[1], args[2], Type_String, Encode_Raw);
        cmd->ReplyOk();
      },
      nullptr);
}

// append key value
void AppendCommand(std::shared_ptr<CmdArgs> cmd) {
  std::pair<EventLoop *, MemDB *> loop =
      MemSaver::Default()->GetDB(cmd->args()[1]);

  auto db = loop.second;
  loop.first->RunInLoopNoWait(
      [cmd, db](EventLoop *et, std::shared_ptr<void> arg) {
        auto &args = cmd->args();
        auto obj = db->Get(cmd->DbIndex(), args[1]);
        if (obj != nullptr && !CheckAndReply(obj, cmd, Type_String)) {
          return;
        }

        std::shared_ptr<buffer_t> str_value;
        if (obj == nullptr) {
          str_value = args[2];
          db->Set(cmd->DbIndex(), args[1], str_value, Type_String, Encode_Raw);
        } else if (obj->type == Type_String) {
          str_value = GenString(OBJ_STRING(obj), obj->encode);
          int oldlen = str_value->len;
          str_value = copy_buffer(str_value, oldlen + args[2]->len);
          memcpy(str_value->data + oldlen, args[2]->data, args[2]->len);
          OBJ_SET_VALUE(obj, str_value, Type_String, Encode_Raw);
        }

        cmd->ReplyInteger(str_value->len);
      },
      nullptr);
}

// getset key value
void GetSetCommand(std::shared_ptr<CmdArgs> cmd) {
  std::pair<EventLoop *, MemDB *> loop =
      MemSaver::Default()->GetDB(cmd->args()[1]);

  auto db = loop.second;
  loop.first->RunInLoopNoWait(
      [cmd, db](EventLoop *et, std::shared_ptr<void> arg) {
        auto &args = cmd->args();
        auto obj = db->Get(cmd->DbIndex(), args[1]);
        if (obj != nullptr && !CheckAndReply(obj, cmd, Type_String)) {
          return;
        }
        ReplyRedisObj(obj, cmd);

        if (obj == nullptr) {
          db->Set(cmd->DbIndex(), args[1], args[2], Type_String, Encode_Raw);
        } else {
          obj->value = args[2];
          obj->type = Type_String;
          obj->encode = Encode_Raw;
        }
      },
      nullptr);
}

// mget key1 ....
void MGetCommand(std::shared_ptr<CmdArgs> cmd) {
  auto &args = cmd->args();
  int cnt = args.size() - 1;
  auto rets = std::make_shared<MultiResult>(cnt);
  for (int i = 0; i < cnt; i++) {
    std::pair<EventLoop *, MemDB *> loop =
        MemSaver::Default()->GetDB(args[i + 1]);

    auto db = loop.second;
    loop.first->RunInLoopNoWait(
        [i, cmd, rets, db](EventLoop *et, std::shared_ptr<void> arg) {
          auto &args = cmd->args();
          auto obj = db->Get(cmd->DbIndex(), args[i + 1]);
          if (obj != nullptr) {
            rets->str_values[i] = GenString(OBJ_STRING(obj), obj->encode);
          }

          rets->cnt--;
          if (rets->cnt.load() == 0) {
            cmd->ReplyArray(rets->str_values);
          }
        },
        nullptr);
  }
}

// mset key1 value1 ....
void MSetCommand(std::shared_ptr<CmdArgs> cmd) {
  auto &args = cmd->args();
  if (args.size() % 2 != 1) {
    cmd->ReplyError(CmdArgs::g_reply_mset_args_err);
    return;
  }

  int cnt = args.size() / 2;
  auto rets = std::make_shared<MultiResult>(cnt);
  for (int i = 0; i < cnt; i++) {
    std::pair<EventLoop *, MemDB *> loop =
        MemSaver::Default()->GetDB(args[i * 2 + 1]);

    auto db = loop.second;
    loop.first->RunInLoopNoWait(
        [i, cmd, rets, db](EventLoop *et, std::shared_ptr<void> arg) {
          auto &args = cmd->args();
          db->Set(cmd->DbIndex(), args[i * 2 + 1], args[i * 2 + 2], Type_String,
                  Encode_Raw);

          rets->cnt--;
          if (rets->cnt.load() == 0) {
            cmd->ReplyOk();
          }
        },
        nullptr);
  }
}

static void IncrDecrProcess(std::shared_ptr<CmdArgs> cmd, MemDB *db, int num) {
  auto &args = cmd->args();
  auto obj = db->Get(cmd->DbIndex(), args[1]);
  if (obj != nullptr && !CheckAndReply(obj, cmd, Type_String)) {
    return;
  }

  if (obj == nullptr) {
    auto value = make_buffer(sizeof(int64_t));
    BUF_INT64(value) = num;
    obj = db->Set(cmd->DbIndex(), args[1], value, Type_String, Encode_Int);
  } else {
    int64_t oldv;
    if (!GenInt64(OBJ_STRING(obj), obj->encode, oldv)) {
      cmd->ReplyError(CmdArgs::g_reply_integer_err);
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
  ReplyRedisObj(obj, cmd);
}

// incr key
void IncrCommand(std::shared_ptr<CmdArgs> cmd) {
  std::pair<EventLoop *, MemDB *> loop =
      MemSaver::Default()->GetDB(cmd->args()[1]);

  auto db = loop.second;
  loop.first->RunInLoopNoWait(
      [cmd, db](EventLoop *et, std::shared_ptr<void> arg) {
        IncrDecrProcess(cmd, db, 1);
      },
      nullptr);
}  // namespace rockin

// incrby key value
void IncrbyCommand(std::shared_ptr<CmdArgs> cmd) {
  std::pair<EventLoop *, MemDB *> loop =
      MemSaver::Default()->GetDB(cmd->args()[1]);

  auto db = loop.second;
  loop.first->RunInLoopNoWait(
      [cmd, db](EventLoop *et, std::shared_ptr<void> arg) {
        int64_t tmpv;
        auto &args = cmd->args();
        if (StringToInt64(args[2]->data, args[2]->len, &tmpv) == 0) {
          cmd->ReplyError(CmdArgs::g_reply_integer_err);
          return;
        }

        IncrDecrProcess(cmd, db, tmpv);
      },
      nullptr);
}

// decr key
void DecrCommand(std::shared_ptr<CmdArgs> cmd) {
  std::pair<EventLoop *, MemDB *> loop =
      MemSaver::Default()->GetDB(cmd->args()[1]);

  auto db = loop.second;
  loop.first->RunInLoopNoWait(
      [cmd, db](EventLoop *et, std::shared_ptr<void> arg) {
        IncrDecrProcess(cmd, db, -1);
      },
      nullptr);
}

// decr key value
void DecrbyCommand(std::shared_ptr<CmdArgs> cmd) {
  std::pair<EventLoop *, MemDB *> loop =
      MemSaver::Default()->GetDB(cmd->args()[1]);

  auto db = loop.second;
  loop.first->RunInLoopNoWait(
      [cmd, db](EventLoop *et, std::shared_ptr<void> arg) {
        int64_t tmpv;
        auto &args = cmd->args();
        if (StringToInt64(args[2]->data, args[2]->len, &tmpv) == 0) {
          cmd->ReplyError(CmdArgs::g_reply_integer_err);
          return;
        }

        IncrDecrProcess(cmd, db, -tmpv);
      },
      nullptr);
}

static bool GetBitOffset(std::shared_ptr<buffer_t> v,
                         std::shared_ptr<CmdArgs> cmd, int64_t &offset) {
  if (StringToInt64(v->data, v->len, &offset) != 1) {
    cmd->ReplyError(CmdArgs::g_reply_bit_err);
    return false;
  }

  if (offset < 0 || (offset >> 3) >= 512 * 1024 * 1024) {
    cmd->ReplyError(CmdArgs::g_reply_bit_err);
    return false;
  }

  return true;
}

// SETBIT key offset value
void SetBitCommand(std::shared_ptr<CmdArgs> cmd) {
  std::pair<EventLoop *, MemDB *> loop =
      MemSaver::Default()->GetDB(cmd->args()[1]);

  auto db = loop.second;
  loop.first->RunInLoopNoWait(
      [cmd, db](EventLoop *et, std::shared_ptr<void> arg) {
        int64_t offset, on;
        auto &args = cmd->args();
        if (GetBitOffset(args[2], cmd, offset) == false) {
          return;
        }

        if (StringToInt64(args[3]->data, args[3]->len, &on) != 1 || on & ~1) {
          cmd->ReplyError(CmdArgs::g_reply_bit_err);
          return;
        }

        auto obj = db->Get(cmd->DbIndex(), args[1]);
        if (obj != nullptr && !CheckAndReply(obj, cmd, Type_String)) {
          return;
        }

        int byte = offset >> 3;
        std::shared_ptr<buffer_t> str_value;
        if (obj == nullptr) {
          str_value = make_buffer(byte + 1);
          obj = db->Set(cmd->DbIndex(), args[1], str_value, Type_String,
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
        cmd->ReplyInteger((byteval & (1 << bit)) ? 1 : 0);

        byteval &= ~(1 << bit);
        byteval |= ((on & 0x1) << bit);
        str_value->data[byte] = byteval;
      },
      nullptr);
}

// GETBIT key offset
void GetBitCommand(std::shared_ptr<CmdArgs> cmd) {
  std::pair<EventLoop *, MemDB *> loop =
      MemSaver::Default()->GetDB(cmd->args()[1]);

  auto db = loop.second;
  loop.first->RunInLoopNoWait(
      [cmd, db](EventLoop *et, std::shared_ptr<void> arg) {
        int64_t offset;
        auto &args = cmd->args();
        if (GetBitOffset(args[2], cmd, offset) == false) {
          return;
        }

        auto obj = db->Get(cmd->DbIndex(), args[1]);
        if (obj == nullptr) {
          cmd->ReplyInteger(0);
          return;
        }
        if (!CheckAndReply(obj, cmd, Type_String)) {
          return;
        }

        int byte = offset >> 3;
        auto str_value = GenString(OBJ_STRING(obj), obj->encode);
        if (str_value->len < byte + 1) {
          cmd->ReplyInteger(0);
          return;
        }

        int bit = 7 - (offset & 0x7);
        char byteval = str_value->data[byte];
        cmd->ReplyInteger((byteval & (1 << bit)) ? 1 : 0);
      },
      nullptr);
}

// BITCOUNT key [start end]
void BitCountCommand(std::shared_ptr<CmdArgs> cmd) {
  std::pair<EventLoop *, MemDB *> loop =
      MemSaver::Default()->GetDB(cmd->args()[1]);

  auto db = loop.second;
  loop.first->RunInLoopNoWait(
      [cmd, db](EventLoop *et, std::shared_ptr<void> arg) {
        auto &args = cmd->args();
        auto obj = db->Get(cmd->DbIndex(), args[1]);
        if (obj == nullptr) {
          cmd->ReplyInteger(0);
          return;
        }
        if (!CheckAndReply(obj, cmd, Type_String)) {
          return;
        }

        auto str_value = GenString(OBJ_STRING(obj), obj->encode);
        if (args.size() == 2) {
          cmd->ReplyInteger(BitCount(str_value->data, str_value->len));
        } else if (args.size() == 4) {
          int64_t start, end;
          if (StringToInt64(args[2]->data, args[2]->len, &start) != 1 ||
              StringToInt64(args[3]->data, args[3]->len, &end) != 1) {
            cmd->ReplyError(CmdArgs::g_reply_integer_err);
            return;
          }

          if (start < 0 && end < 0 && start > end) {
            cmd->ReplyInteger(0);
            return;
          }

          if (start < 0) start = str_value->len + start;
          if (end < 0) end = str_value->len + end;
          if (start < 0) start = 0;
          if (end < 0) end = 0;
          if (end >= str_value->len) end = str_value->len - 1;
          if (start > end) {
            cmd->ReplyInteger(0);
          } else {
            cmd->ReplyInteger(
                BitCount(str_value->data + start, end - start + 1));
          }
        } else {
          cmd->ReplyError(CmdArgs::g_reply_syntax_err);
        }
      },
      nullptr);
}

// BITOP AND destkey srckey1 srckey2 srckey3 ... srckeyN
// BITOP OR destkey srckey1 srckey2 srckey3... srckeyN
// BITOP XOR destkey srckey1 srckey2 srckey3... srckeyN
// BITOP NOT destkey srckey
#define BITOP_AND 0
#define BITOP_OR 1
#define BITOP_XOR 2
#define BITOP_NOT 3

void BitOpCommand(std::shared_ptr<CmdArgs> cmd) {
  std::pair<EventLoop *, MemDB *> loop =
      MemSaver::Default()->GetDB(cmd->args()[1]);

  auto db = loop.second;
  loop.first->RunInLoopNoWait(
      [cmd, db](EventLoop *et, std::shared_ptr<void> arg) {
        auto &args = cmd->args();
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
          cmd->ReplyError(CmdArgs::g_reply_syntax_err);
          return;
        }

        if (op == BITOP_NOT && args.size() != 4) {
          cmd->ReplyError(CmdArgs::g_reply_syntax_err);
          return;
        }

        size_t maxlen = 0;
        std::vector<std::shared_ptr<buffer_t>> values;
        for (int i = 3; i < args.size(); i++) {
          auto obj = db->Get(cmd->DbIndex(), args[i]);
          if (obj == nullptr) {
            values.push_back(nullptr);
          } else {
            if (!CheckAndReply(obj, cmd, Type_String)) {
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

          db->Set(cmd->DbIndex(), args[1], res, Type_String, Encode_Raw);
        }

        cmd->ReplyInteger(maxlen);
      },
      nullptr);
}

// BITPOS key bit [start][end]
void BitPosCommand(std::shared_ptr<CmdArgs> cmd) {
  std::pair<EventLoop *, MemDB *> loop =
      MemSaver::Default()->GetDB(cmd->args()[1]);

  auto db = loop.second;
  loop.first->RunInLoopNoWait(
      [cmd, db](EventLoop *et, std::shared_ptr<void> arg) {
        auto &args = cmd->args();
        int64_t bit = 0;
        if (StringToInt64(args[2]->data, args[2]->len, &bit) != 1 || bit & ~1) {
          cmd->ReplyError(CmdArgs::g_reply_integer_err);
          return;
        }

        auto obj = db->Get(cmd->DbIndex(), args[1]);
        if (obj == nullptr) {
          cmd->ReplyInteger(bit ? -1 : 0);
          return;
        }
        if (!CheckAndReply(obj, cmd, Type_String)) {
          return;
        }

        auto str_value = GenString(OBJ_STRING(obj), obj->encode);
        bool end_given = false;
        int64_t start, end;
        if (args.size() == 4 || args.size() == 5) {
          if (StringToInt64(args[3]->data, args[3]->len, &start) != 1) {
            cmd->ReplyError(CmdArgs::g_reply_integer_err);
            return;
          }
          if (args.size() == 5) {
            if (StringToInt64(args[4]->data, args[4]->len, &end) != 1) {
              cmd->ReplyError(CmdArgs::g_reply_integer_err);
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
          cmd->ReplyError(CmdArgs::g_reply_syntax_err);
          return;
        }

        if (start > end) {
          cmd->ReplyInteger(-1);
          return;
        }

        int bytes = end - start + 1;
        long pos = Bitpos(str_value->data + start, bytes, bit);

        if (end_given && bit == 0 && pos == bytes * 8) {
          cmd->ReplyInteger(-1);
          return;
        }

        if (pos != -1) pos += start * 8; /* Adjust for the bytes we skipped. */
        cmd->ReplyInteger(pos);
      },
      nullptr);
}

}  // namespace rockin