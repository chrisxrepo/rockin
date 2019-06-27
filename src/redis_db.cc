#include "redis_db.h"
#include "redis_cmd.h"
#include "redis_string.h"

namespace rockin {

RedisDB::RedisDB() {}

RedisDB::~RedisDB() {}

std::shared_ptr<RedisObj> RedisDB::Get(std::shared_ptr<buffer_t> key) {
  return dic_.Get(key);
}

std::shared_ptr<RedisObj> RedisDB::GetReplyNil(std::shared_ptr<buffer_t> key,
                                               std::shared_ptr<RedisCmd> cmd) {
  auto obj = Get(key);
  if (obj == nullptr) {
    cmd->ReplyNil();
    return nullptr;
  }

  return obj;
}

void RedisDB::Set(std::shared_ptr<buffer_t> key, std::shared_ptr<void> value,
                  ValueType type) {
  auto obj = dic_.Get(key);
  if (obj == nullptr) {
    obj = std::make_shared<RedisObj>(type, std::move(key), value);
    dic_.Insert(obj);
  } else {
    obj->type_ = type;
    obj->value_ = value;
  }
}

void RedisDB::SetObj(std::shared_ptr<RedisObj> obj, std::shared_ptr<void> value,
                     ValueType type) {
  obj->value_ = value;
  obj->type_ = type;
}

// delete by key
bool RedisDB::Delete(std::shared_ptr<buffer_t> key) { return dic_.Delete(key); }

bool CheckAndReply(std::shared_ptr<RedisObj> obj, std::shared_ptr<RedisCmd> cmd,
                   int type) {
  if (obj->type() & type) {
    return true;
  }

  cmd->ReplyError(RedisCmd::g_reply_type_warn);
  return false;
}

void ReplyRedisObj(std::shared_ptr<RedisObj> obj,
                   std::shared_ptr<RedisCmd> cmd) {
  if (obj == nullptr) {
    cmd->ReplyNil();
  } else if (obj->type() == TypeString) {
    auto str_value = std::static_pointer_cast<RedisString>(obj->value());
    cmd->ReplyBulk(str_value->Value());
  } else if (obj->type() == TypeInteger) {
    auto int_value = std::static_pointer_cast<RedisInteger>(obj->value());
    cmd->ReplyBulk(make_buffer(Int64ToString(int_value->Value())));
  }
}

std::shared_ptr<buffer_t> ObjToString(std::shared_ptr<RedisObj> obj) {
  if (obj->type() & TypeString) {
    auto str_value = std::static_pointer_cast<RedisString>(obj->value());
    return str_value->Value();
  } else if (obj->type() & TypeInteger) {
    auto int_value = std::static_pointer_cast<RedisInteger>(obj->value());
    return make_buffer(Int64ToString(int_value->Value()));
  }
  return nullptr;
}

bool ObjToInt64(std::shared_ptr<RedisObj> obj, int64_t &v) {
  if (obj->type() & TypeString) {
    auto str_value = std::static_pointer_cast<RedisString>(obj->value());
    if (StringToInt64(str_value->Value()->data, str_value->Value()->len, &v) ==
        0) {
      return false;
    }
    return true;
  } else if (obj->type() & TypeInteger) {
    auto int_value = std::static_pointer_cast<RedisInteger>(obj->value());
    v = int_value->Value();
    return true;
  }
  return false;
}

bool ObjToDouble(std::shared_ptr<RedisObj> obj, double &v) {
  if (obj->type() & TypeString) {
    auto str_value = std::static_pointer_cast<RedisString>(obj->value());
    if (StringToDouble(str_value->Value()->data, str_value->Value()->len, &v) ==
        0) {
      return false;
    }
    return true;
  } else if (obj->type() & TypeInteger) {
    auto int_value = std::static_pointer_cast<RedisInteger>(obj->value());
    v = int_value->Value();
    return true;
  }
  return false;
}

}  // namespace rockin