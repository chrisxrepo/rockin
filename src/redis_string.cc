#include "redis_string.h"
#include <glog/logging.h>
#include <math.h>
#include "redis_cmd.h"
#include "redis_common.h"
#include "redis_db.h"
#include "redis_pool.h"

namespace rockin {

std::shared_ptr<buffer_t> ObjToString(std::shared_ptr<RedisObj> obj) {
  if (obj->type() & RedisObj::String) {
    auto str_value = std::static_pointer_cast<RedisString>(obj->value());
    if (obj->encode() == RedisString::Int) {
      return make_buffer(Int64ToString(str_value->IntValue()));
    } else {
      return str_value->Value();
    }
  }
  return nullptr;
}

bool ObjToInt64(std::shared_ptr<RedisObj> obj, int64_t &v) {
  if (obj->type() & RedisObj::String) {
    auto str_value = std::static_pointer_cast<RedisString>(obj->value());
    if (obj->encode() == RedisString::Int) {
      v = str_value->IntValue();
      return true;
    } else {
      if (StringToInt64(str_value->Value()->data, str_value->Value()->len, &v))
        return true;
    }
  }
  return false;
}

std::shared_ptr<RedisString> GenRedisStringRaw(RedisDB *db,
                                               std::shared_ptr<RedisObj> obj) {
  if (obj->type() != RedisObj::String) return nullptr;

  auto str_value = std::static_pointer_cast<RedisString>(obj->value());
  if (obj->encode() == RedisString::Raw) return str_value;

  auto new_value = std::make_shared<RedisString>(
      make_buffer(Int64ToString(str_value->IntValue())));
  db->SetObj(obj, new_value, RedisObj::String, RedisString::Raw);

  return new_value;
}

// get key
void GetCommand(std::shared_ptr<RedisCmd> cmd) {
  std::pair<EventLoop *, RedisDB *> db =
      RedisPool::GetInstance()->GetDB(cmd->Args()[1]);

  db.first->RunInLoopNoWait([cmd, db](EventLoop *el) {
    auto obj = db.second->GetReplyNil(cmd->Args()[1], cmd);
    if (obj == nullptr || !CheckAndReply(obj, cmd, RedisObj::String)) {
      return;
    }

    cmd->ReplyBulk(ObjToString(obj));
  });
}

// set key value
void SetCommand(std::shared_ptr<RedisCmd> cmd) {
  std::pair<EventLoop *, RedisDB *> db =
      RedisPool::GetInstance()->GetDB(cmd->Args()[1]);

  db.first->RunInLoopNoWait([cmd, db](EventLoop *el) {
    auto &args = cmd->Args();
    auto obj = std::make_shared<RedisString>(args[2]);
    db.second->Set(args[1], obj, RedisObj::String, RedisString::Raw);
    cmd->ReplyOk();
  });
}

// append key value
void AppendCommand(std::shared_ptr<RedisCmd> cmd) {
  std::pair<EventLoop *, RedisDB *> db =
      RedisPool::GetInstance()->GetDB(cmd->Args()[1]);

  db.first->RunInLoopNoWait([cmd, db](EventLoop *el) {
    auto &args = cmd->Args();
    auto obj = db.second->Get(args[1]);
    if (obj != nullptr && !CheckAndReply(obj, cmd, RedisObj::String)) {
      return;
    }

    std::shared_ptr<RedisString> str_value;
    if (obj == nullptr) {
      str_value = std::make_shared<RedisString>(args[2]);
      db.second->Set(std::move(args[1]), str_value, RedisObj::String,
                     RedisString::Raw);
    } else if (obj->type() == RedisObj::String) {
      auto old_value = ObjToString(obj);
      auto new_value = make_buffer(old_value->len + args[2]->len);
      memcpy(new_value->data, old_value->data, old_value->len);
      memcpy(new_value->data + old_value->len, args[2]->data, args[2]->len);
      str_value = std::make_shared<RedisString>(new_value);
      db.second->SetObj(obj, str_value, RedisObj::String, RedisString::Raw);
    }

    cmd->ReplyInteger(str_value->Value()->len);
  });
}

// getset key value
void GetSetCommand(std::shared_ptr<RedisCmd> cmd) {
  std::pair<EventLoop *, RedisDB *> db =
      RedisPool::GetInstance()->GetDB(cmd->Args()[1]);

  db.first->RunInLoopNoWait([cmd, db](EventLoop *el) {
    auto &args = cmd->Args();
    auto obj = db.second->Get(args[1]);
    if (obj != nullptr && !CheckAndReply(obj, cmd, RedisObj::String)) {
      return;
    }
    ReplyRedisObj(obj, cmd);

    auto new_value = std::make_shared<RedisString>(args[2]);
    if (obj == nullptr) {
      db.second->Set(args[1], new_value, RedisObj::String, RedisString::Raw);
    } else {
      db.second->SetObj(obj, new_value, RedisObj::String, RedisString::Raw);
    }
  });
}

// mget key1 ....
void MGetCommand(std::shared_ptr<RedisCmd> cmd) {
  auto &args = cmd->Args();
  int cnt = args.size() - 1;
  auto rets = std::make_shared<MultiResult>(cnt);
  for (int i = 0; i < cnt; i++) {
    std::pair<EventLoop *, RedisDB *> db =
        RedisPool::GetInstance()->GetDB(args[i + 1]);

    db.first->RunInLoopNoWait([i, cmd, rets, db](EventLoop *el) {
      auto &args = cmd->Args();
      auto obj = db.second->Get(args[i + 1]);
      if (obj != nullptr) {
        rets->str_values[i] = ObjToString(obj);
      }

      rets->cnt--;
      if (rets->cnt.load() == 0) {
        cmd->ReplyArray(rets->str_values);
      }
    });
  }
}

// mset key1 value1 ....
void MSetCommand(std::shared_ptr<RedisCmd> cmd) {
  auto &args = cmd->Args();
  if (args.size() % 2 != 1) {
    cmd->ReplyError(RedisCmd::g_reply_mset_args_err);
    return;
  }

  int cnt = args.size() / 2;
  auto rets = std::make_shared<MultiResult>(cnt);
  for (int i = 0; i < cnt; i++) {
    std::pair<EventLoop *, RedisDB *> db =
        RedisPool::GetInstance()->GetDB(args[i * 2 + 1]);

    db.first->RunInLoopNoWait([i, cmd, rets, db](EventLoop *el) {
      auto &args = cmd->Args();
      auto str_value = std::make_shared<RedisString>(args[i * 2 + 2]);
      db.second->Set(args[i * 2 + 1], str_value, RedisObj::String,
                     RedisString::Raw);

      rets->cnt--;
      if (rets->cnt.load() == 0) {
        cmd->ReplyOk();
      }
    });
  }
}

static void IncrDecrProcess(std::shared_ptr<RedisCmd> cmd, RedisDB *db,
                            int num) {
  auto &args = cmd->Args();
  auto obj = db->Get(args[1]);
  if (obj != nullptr && !CheckAndReply(obj, cmd, RedisObj::String)) {
    return;
  }

  if (obj == nullptr) {
    auto value = std::make_shared<RedisString>(num);
    obj = db->Set(args[1], value, RedisObj::String, RedisString::Int);
  } else {
    int64_t oldv;
    if (!ObjToInt64(obj, oldv)) {
      cmd->ReplyError(RedisCmd::g_reply_integer_err);
      return;
    }

    if (obj->encode() == RedisString::Int) {
      auto value = std::static_pointer_cast<RedisString>(obj->value());
      value->SetIntValue(oldv + num);
    } else {
      auto value = std::make_shared<RedisString>(oldv + num);
      db->SetObj(obj, value, RedisObj::String, RedisString::Int);
    }
  }
  ReplyRedisObj(obj, cmd);
}

// incr key
void IncrCommand(std::shared_ptr<RedisCmd> cmd) {
  std::pair<EventLoop *, RedisDB *> db =
      RedisPool::GetInstance()->GetDB(cmd->Args()[1]);

  db.first->RunInLoopNoWait(
      [cmd, db](EventLoop *el) { IncrDecrProcess(cmd, db.second, 1); });
}  // namespace rockin

// incrby key value
void IncrbyCommand(std::shared_ptr<RedisCmd> cmd) {
  std::pair<EventLoop *, RedisDB *> db =
      RedisPool::GetInstance()->GetDB(cmd->Args()[1]);

  db.first->RunInLoopNoWait([cmd, db](EventLoop *el) {
    int64_t tmpv;
    auto &args = cmd->Args();
    if (StringToInt64(args[2]->data, args[2]->len, &tmpv) == 0) {
      cmd->ReplyError(RedisCmd::g_reply_integer_err);
      return;
    }

    IncrDecrProcess(cmd, db.second, tmpv);
  });
}

// decr key
void DecrCommand(std::shared_ptr<RedisCmd> cmd) {
  std::pair<EventLoop *, RedisDB *> db =
      RedisPool::GetInstance()->GetDB(cmd->Args()[1]);

  db.first->RunInLoopNoWait(
      [cmd, db](EventLoop *el) { IncrDecrProcess(cmd, db.second, -1); });
}

// decr key value
void DecrbyCommand(std::shared_ptr<RedisCmd> cmd) {
  std::pair<EventLoop *, RedisDB *> db =
      RedisPool::GetInstance()->GetDB(cmd->Args()[1]);

  db.first->RunInLoopNoWait([cmd, db](EventLoop *el) {
    int64_t tmpv;
    auto &args = cmd->Args();
    if (StringToInt64(args[2]->data, args[2]->len, &tmpv) == 0) {
      cmd->ReplyError(RedisCmd::g_reply_integer_err);
      return;
    }

    IncrDecrProcess(cmd, db.second, -tmpv);
  });
}

static bool GetBitOffset(std::shared_ptr<buffer_t> v,
                         std::shared_ptr<RedisCmd> cmd, int64_t &offset) {
  if (StringToInt64(v->data, v->len, &offset) != 1) {
    cmd->ReplyError(RedisCmd::g_reply_bit_err);
    return false;
  }

  if (offset < 0 || (offset >> 3) >= 512 * 1024 * 1024) {
    cmd->ReplyError(RedisCmd::g_reply_bit_err);
    return false;
  }

  return true;
}

// SETBIT key offset value
void SetBitCommand(std::shared_ptr<RedisCmd> cmd) {
  std::pair<EventLoop *, RedisDB *> db =
      RedisPool::GetInstance()->GetDB(cmd->Args()[1]);

  db.first->RunInLoopNoWait([cmd, db](EventLoop *el) {
    int64_t offset, on;
    auto &args = cmd->Args();
    if (GetBitOffset(args[2], cmd, offset) == false) {
      return;
    }

    if (StringToInt64(args[3]->data, args[3]->len, &on) != 1 || on & ~1) {
      cmd->ReplyError(RedisCmd::g_reply_bit_err);
      return;
    }

    auto obj = db.second->Get(args[1]);
    if (obj != nullptr && !CheckAndReply(obj, cmd, RedisObj::String)) {
      return;
    }

    int byte = offset >> 3;
    std::shared_ptr<RedisString> str_value;
    if (obj == nullptr) {
      str_value = std::make_shared<RedisString>(make_buffer(byte + 1));
      obj = db.second->Set(args[1], str_value, RedisObj::String,
                           RedisString::Raw);
    } else {
      str_value = GenRedisStringRaw(db.second, obj);
    }

    if (str_value->Value()->len < byte + 1) {
      auto new_value = std::make_shared<RedisString>(make_buffer(byte + 1));
      memcpy(new_value->Value()->data, str_value->Value()->data,
             str_value->Value()->len);

      db.second->SetObj(obj, new_value, RedisObj::String, RedisString::Raw);
      str_value = new_value;
    }

    int bit = 7 - (offset & 0x7);
    char byteval = str_value->Value()->data[byte];
    cmd->ReplyInteger((byteval & (1 << bit)) ? 1 : 0);

    byteval &= ~(1 << bit);
    byteval |= ((on & 0x1) << bit);
    str_value->Value()->data[byte] = byteval;
  });
}

// GETBIT key offset
void GetBitCommand(std::shared_ptr<RedisCmd> cmd) {
  std::pair<EventLoop *, RedisDB *> db =
      RedisPool::GetInstance()->GetDB(cmd->Args()[1]);

  db.first->RunInLoopNoWait([cmd, db](EventLoop *el) {
    int64_t offset;
    auto &args = cmd->Args();
    if (GetBitOffset(args[2], cmd, offset) == false) {
      return;
    }

    auto obj = db.second->Get(args[1]);
    if (obj == nullptr) {
      cmd->ReplyInteger(0);
      return;
    }
    if (!CheckAndReply(obj, cmd, RedisObj::String)) {
      return;
    }

    int byte = offset >> 3;
    auto str_value = GenRedisStringRaw(db.second, obj);
    if (str_value->Value()->len < byte + 1) {
      cmd->ReplyInteger(0);
      return;
    }

    int bit = 7 - (offset & 0x7);
    char byteval = str_value->Value()->data[byte];
    cmd->ReplyInteger((byteval & (1 << bit)) ? 1 : 0);
  });
}

// BITCOUNT key [start end]
void BitCountCommand(std::shared_ptr<RedisCmd> cmd) {}

// BITOP AND destkey srckey1 srckey2 srckey3 ... srckeyN
// BITOP OR destkey srckey1 srckey2 srckey3... srckeyN
// BITOP XOR destkey srckey1 srckey2 srckey3... srckeyN
// BITOP NOT destkey srckey
void BitOpCommand(std::shared_ptr<RedisCmd> cmd) {}

// BITPOS key bit[start][end]
void BitOpsCommand(std::shared_ptr<RedisCmd> cmd) {}

}  // namespace rockin
