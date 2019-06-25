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
    auto obj = std::make_shared<RedisString>(std::move(cmd->Args()[2]));
    db.second->Set(std::move(cmd->Args()[1]), obj, TypeString);
    cmd->ReplyString("OK");
  });
}

// append key value
void AppendCommand(std::shared_ptr<RedisCmd> cmd) {
  std::pair<EventLoop *, RedisDB *> db =
      RedisPool::GetInstance()->GetDB(cmd->Args()[1]);

  db.first->RunInLoopNoWait([cmd, db](EventLoop *el) {
    auto obj = db.second->Get(cmd->Args()[1]);
    if (obj != nullptr && !CheckAndReply(obj, cmd, TypeString | TypeInteger)) {
      return;
    }

    std::shared_ptr<RedisString> str_value;
    if (obj == nullptr) {
      str_value = std::make_shared<RedisString>(std::move(cmd->Args()[2]));
      db.second->Set(std::move(cmd->Args()[1]), str_value, TypeString);
    } else if (obj->type() == TypeString) {
      str_value = std::static_pointer_cast<RedisString>(obj->value());
      str_value->Append(cmd->Args()[2]);
    } else if (obj->type() == TypeInteger) {
      auto int_value = std::static_pointer_cast<RedisInteger>(obj->value());
      str_value = std::make_shared<RedisString>(
          Int64ToString(int_value->Value()) + cmd->Args()[2]);
      db.second->SetObj(obj, str_value, TypeString);
    }

    cmd->ReplyInteger(str_value->Value().length());
  });
}

// getset key value
void GetSetCommand(std::shared_ptr<RedisCmd> cmd) {
  std::pair<EventLoop *, RedisDB *> db =
      RedisPool::GetInstance()->GetDB(cmd->Args()[1]);

  db.first->RunInLoopNoWait([cmd, db](EventLoop *el) {
    auto obj = db.second->Get(cmd->Args()[1]);
    if (obj != nullptr && !CheckAndReply(obj, cmd, TypeString | TypeInteger)) {
      return;
    }
    ReplyRedisObj(obj, cmd);

    auto new_value = std::make_shared<RedisString>(std::move(cmd->Args()[2]));
    if (obj == nullptr) {
      db.second->Set(std::move(cmd->Args()[1]), new_value, TypeString);
    } else {
      obj->value() = new_value;
    }
  });
}

// mget key1 ....
void MGetCommand(std::shared_ptr<RedisCmd> cmd) {
  int cnt = cmd->Args().size() - 1;
  auto rets = std::make_shared<MultiResult>(cnt);
  for (int i = 0; i < cnt; i++) {
    std::pair<EventLoop *, RedisDB *> db =
        RedisPool::GetInstance()->GetDB(cmd->Args()[i + 1]);

    db.first->RunInLoopNoWait([i, cmd, rets, db](EventLoop *el) {
      auto obj = db.second->Get(cmd->Args()[i + 1]);
      if (obj == nullptr) {
        rets->exists[i] = false;
      } else {
        std::string v;
        rets->exists[i] = ObjToString(obj, v);
        rets->values[i] = std::move(v);
      }

      rets->cnt--;
      if (rets->cnt.load() == 0) {
        cmd->ReplyArray(rets->values, rets->exists);
      }
    });
  }
}

// mset key1 value1 ....
void MSetCommand(std::shared_ptr<RedisCmd> cmd) {
  if (cmd->Args().size() % 2 != 1) {
    cmd->ReplyError("ERR wrong number of arguments for MSET");
    return;
  }

  int cnt = cmd->Args().size() / 2;
  auto rets = std::make_shared<MultiResult>(cnt);
  for (int i = 0; i < cnt; i++) {
    std::pair<EventLoop *, RedisDB *> db =
        RedisPool::GetInstance()->GetDB(cmd->Args()[i * 2 + 1]);

    db.first->RunInLoopNoWait([i, cmd, rets, db](EventLoop *el) {
      auto str_value =
          std::make_shared<RedisString>(std::move(cmd->Args()[i * 2 + 2]));
      db.second->Set(std::move(cmd->Args()[i * 2 + 1]), str_value, TypeString);

      rets->cnt--;
      if (rets->cnt.load() == 0) {
        cmd->ReplyString("OK");
      }
    });
  }
}

static void IncrDecrProcess(std::shared_ptr<RedisCmd> cmd, RedisDB *db,
                            int num) {
  auto obj = db->Get(cmd->Args()[1]);
  if (obj != nullptr && !CheckAndReply(obj, cmd, TypeString | TypeInteger)) {
    return;
  }

  std::shared_ptr<RedisInteger> int_value;
  if (obj == nullptr) {
    int_value = std::make_shared<RedisInteger>(num);
    db->Set(std::move(cmd->Args()[1]), int_value, TypeInteger);
  } else {
    int64_t oldv;
    if (!ObjToInt64(obj, oldv)) {
      cmd->ReplyError("ERR value is not an integer or out of range");
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
    if (StringToInt64(cmd->Args()[2].c_str(), cmd->Args()[2].length(), &tmpv) ==
        0) {
      cmd->ReplyError("ERR value is not an integer or out of range");
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
    if (StringToInt64(cmd->Args()[2].c_str(), cmd->Args()[2].length(), &tmpv) ==
        0) {
      cmd->ReplyError("ERR value is not an integer or out of range");
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
    if (StringToDouble(cmd->Args()[2].c_str(), cmd->Args()[2].length(),
                       &addv) == 0) {
      cmd->ReplyError("ERR value is not a valid float");
      return;
    }

    auto obj = db.second->Get(cmd->Args()[1]);
    if (obj != nullptr && !CheckAndReply(obj, cmd, TypeString | TypeInteger)) {
      return;
    }

    std::shared_ptr<RedisString> str_value;
    if (obj == nullptr) {
      str_value = std::make_shared<RedisString>(DoubleToString(addv, 1));
      db.second->Set(std::move(cmd->Args()[1]), str_value, TypeString);
    } else {
      double v;
      if (!ObjToDouble(obj, v)) {
        cmd->ReplyError("ERR value is not a valid float");
        return;
      }

      v += addv;
      if (isnan(v) || isinf(v)) {
        cmd->ReplyError("ERR increment would produce NaN or Infinity");
        return;
      }

      if (obj->type() == TypeString) {
        str_value = std::static_pointer_cast<RedisString>(obj->value());
        str_value->SetValue(DoubleToString(v, 1));
      } else {
        str_value = std::make_shared<RedisString>(DoubleToString(v, 1));
      }

      obj->value() = str_value;
    }
    cmd->ReplyString(std::string(str_value->Value()));
  });
}

}  // namespace rockin
