#include "redis_string.h"
#include <glog/logging.h>
#include <math.h>
#include "redis_cmd.h"
#include "redis_common.h"
#include "redis_db.h"
#include "redis_pool.h"

namespace rockin {
// get key
void GetCommand(std::shared_ptr<RedisCmd> cmd) {
  std::pair<EventLoop *, RedisDB *> db =
      RedisPool::GetInstance()->GetDB(cmd->Args()[1]);

  db.first->RunInLoopNoWait([cmd, db](EventLoop *el) {
    auto obj = db.second->GetReplyNil(cmd->Args()[1], cmd);
    if (obj == nullptr || !CheckAndReply(obj, cmd, TypeString | TypeInteger)) {
      return;
    }

    ReplyRedisObj(obj, cmd);
  });
}

// set key value
void SetCommand(std::shared_ptr<RedisCmd> cmd) {
  std::pair<EventLoop *, RedisDB *> db =
      RedisPool::GetInstance()->GetDB(cmd->Args()[1]);

  db.first->RunInLoopNoWait([cmd, db](EventLoop *el) {
    auto &args = cmd->Args();
    auto obj = std::make_shared<RedisString>(args[2]);
    db.second->Set(args[1], obj, TypeString);
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
    if (obj != nullptr && !CheckAndReply(obj, cmd, TypeString | TypeInteger)) {
      return;
    }

    std::shared_ptr<RedisString> str_value;
    if (obj == nullptr) {
      str_value = std::make_shared<RedisString>(args[2]);
      db.second->Set(std::move(args[1]), str_value, TypeString);
    } else if (obj->type() == TypeString) {
      str_value = std::static_pointer_cast<RedisString>(obj->value());
      auto old_value = str_value->Value();
      auto new_value = make_buffer(old_value->len + args[2]->len);
      memcpy(new_value->data, old_value->data, old_value->len);
      memcpy(new_value->data + old_value->len, args[2]->data, args[2]->len);
      str_value = std::make_shared<RedisString>(new_value);
      db.second->SetObj(obj, str_value, TypeString);
    } else if (obj->type() == TypeInteger) {
      auto int_value = std::static_pointer_cast<RedisInteger>(obj->value());
      std::string str = Int64ToString(int_value->Value());
      auto buf = make_buffer(str.length() + args[2]->len);
      memcpy(buf->data, str.c_str(), str.length());
      memcpy(buf->data + str.length(), args[2]->data, args[2]->len);
      str_value = std::make_shared<RedisString>(buf);
      db.second->SetObj(obj, str_value, TypeString);
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
    if (obj != nullptr && !CheckAndReply(obj, cmd, TypeString | TypeInteger)) {
      return;
    }
    ReplyRedisObj(obj, cmd);

    auto new_value = std::make_shared<RedisString>(args[2]);
    if (obj == nullptr) {
      db.second->Set(args[1], new_value, TypeString);
    } else {
      obj->value() = new_value;
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
      db.second->Set(args[i * 2 + 1], str_value, TypeString);

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
  if (obj != nullptr && !CheckAndReply(obj, cmd, TypeString | TypeInteger)) {
    return;
  }

  std::shared_ptr<RedisInteger> int_value;
  if (obj == nullptr) {
    int_value = std::make_shared<RedisInteger>(num);
    db->Set(args[1], int_value, TypeInteger);
  } else {
    int64_t oldv;
    if (!ObjToInt64(obj, oldv)) {
      cmd->ReplyError(RedisCmd::g_reply_integer_err);
      return;
    }

    if (obj->type() == TypeInteger) {
      int_value = std::static_pointer_cast<RedisInteger>(obj->value());
      int_value->SetValue(oldv + num);
    } else {
      int_value = std::make_shared<RedisInteger>(oldv + num);
    }
    db->SetObj(obj, int_value, TypeInteger);
  }

  cmd->ReplyInteger(int_value->Value());
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

// decr key value
void IncrbyFloatCommand(std::shared_ptr<RedisCmd> cmd) {
  std::pair<EventLoop *, RedisDB *> db =
      RedisPool::GetInstance()->GetDB(cmd->Args()[1]);

  db.first->RunInLoopNoWait([cmd, db](EventLoop *el) {
    double addv;
    auto &args = cmd->Args();
    if (StringToDouble(args[2]->data, args[2]->len, &addv) == 0) {
      cmd->ReplyError(RedisCmd::g_reply_float_err);
      return;
    }

    auto obj = db.second->Get(args[1]);
    if (obj != nullptr && !CheckAndReply(obj, cmd, TypeString | TypeInteger)) {
      return;
    }

    std::shared_ptr<RedisString> str_value;
    if (obj == nullptr) {
      str_value =
          std::make_shared<RedisString>(make_buffer(DoubleToString(addv, 1)));
      db.second->Set(std::move(args[1]), str_value, TypeString);
    } else {
      double v;
      if (!ObjToDouble(obj, v)) {
        cmd->ReplyError(RedisCmd::g_reply_float_err);
        return;
      }

      v += addv;
      if (isnan(v) || isinf(v)) {
        cmd->ReplyError(RedisCmd::g_reply_nan_err);
        return;
      }

      if (obj->type() == TypeString) {
        str_value = std::static_pointer_cast<RedisString>(obj->value());
        str_value->SetValue(make_buffer(DoubleToString(v, 1)));
      } else {
        str_value =
            std::make_shared<RedisString>(make_buffer(DoubleToString(v, 1)));
      }

      obj->value() = str_value;
    }
    cmd->ReplyString(str_value->Value());
  });
}

static bool GetBitOffset(const std::string &str, std::shared_ptr<RedisCmd> cmd,
                         int64_t &offset) {
  if (StringToInt64(str.c_str(), str.length(), &offset) != 1) {
    cmd->ReplyError("bit offset is not an integer or out of range");
    return false;
  }

  if (offset < 0 || (offset >> 3) >= 512 * 1024 * 1024) {
    cmd->ReplyError("bit offset is not an integer or out of range");
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

    if (StringToInt64(args[3].c_str(), args[3].length(), &on) != 1 || on & ~1) {
      cmd->ReplyError("bit is not an integer or out of range");
      return;
    }

    auto obj = db.second->Get(args[1]);
    if (obj != nullptr && !CheckAndReply(obj, cmd, TypeString | TypeInteger)) {
      return;
    }
    
  });
}

// GETBIT key offset
void GetBitCommand(std::shared_ptr<RedisCmd> cmd) {
  std::pair<EventLoop *, RedisDB *> db =
      RedisPool::GetInstance()->GetDB(cmd->Args()[1]);

  db.first->RunInLoopNoWait([cmd, db](EventLoop *el) {});
}

// BITCOUNT key [start end]
void BitCountCommand(std::shared_ptr<RedisCmd> cmd) {
  std::pair<EventLoop *, RedisDB *> db =
      RedisPool::GetInstance()->GetDB(cmd->Args()[1]);

  db.first->RunInLoopNoWait([cmd, db](EventLoop *el) {
    auto &args = cmd->Args();
    if (args.size() != 2 && args.size() != 4) {
      cmd->ReplyError("ERR syntax error");
      return;
    }

    auto obj = db.second->Get(args[1]);
    if (obj == nullptr) {
      cmd->ReplyInteger(0);
      return;
    }
    if (!CheckAndReply(obj, cmd, TypeString | TypeInteger)) {
      return;
    }

    std::string str;
    ObjToString(obj, str);
    if (args.size() == 2) {
      cmd->ReplyInteger(BitCount((void *)str.c_str(), str.length()));
    } else {
      int64_t start, end;
      if (StringToInt64(args[2].c_str(), args[2].length(), &start) != 1 &&
          StringToInt64(args[3].c_str(), args[3].length(), &end) != 1) {
        cmd->ReplyError("ERR value is not an integer or out of range");
        return;
      }
      if (start < 0 && end < 0 && start > end) {
        cmd->ReplyInteger(0);
        return;
      }

      if (start < 0) start = str.length() + start;
      if (end < 0) end = str.length() + end;
      if (start < 0) start = 0;
      if (end < 0) end = 0;
      if (end >= str.length()) end = str.length() - 1;
      if (start > end) {
        cmd->ReplyInteger(0);
      } else {
        cmd->ReplyInteger(
            BitCount((void *)(str.c_str() + start), end - start + 1));
      }
    }
  });
}

// BITOP AND destkey srckey1 srckey2 srckey3 ... srckeyN
// BITOP OR destkey srckey1 srckey2 srckey3... srckeyN
// BITOP XOR destkey srckey1 srckey2 srckey3... srckeyN
// BITOP NOT destkey srckey
void BitOpCommand(std::shared_ptr<RedisCmd> cmd) {}

// BITPOS key bit[start][end]
void BitOpsCommand(std::shared_ptr<RedisCmd> cmd) {}

}  // namespace rockin
