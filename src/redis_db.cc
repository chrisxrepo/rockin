#include "redis_db.h"
#include "redis_cmd.h"
#include "redis_string.h"

namespace rockin {

RedisDB::RedisDB() {}

RedisDB::~RedisDB() {}

std::shared_ptr<RedisObj> RedisDB::Get(const std::string &key) {
  return dic_.Get(key);
}

std::shared_ptr<RedisObj> RedisDB::GetReplyNil(const std::string &key,
                                               std::shared_ptr<RedisCmd> cmd) {
  auto obj = Get(key);
  if (obj == nullptr) {
    cmd->ReplyNil();
    return nullptr;
  }

  return obj;
}

void RedisDB::Set(std::string &&key, std::shared_ptr<void> value,
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
bool RedisDB::Delete(const std::string &key) { return dic_.Delete(key); }

bool CheckAndReply(std::shared_ptr<RedisObj> obj, std::shared_ptr<RedisCmd> cmd,
                   int type) {
  if (obj->type() & type) {
    return true;
  }

  cmd->ReplyTypeWaring();
  return false;
}

void ReplyRedisObj(std::shared_ptr<RedisObj> obj,
                   std::shared_ptr<RedisCmd> cmd) {
  if (obj == nullptr) {
    cmd->ReplyNil();
  } else if (obj->type() == TypeString) {
    auto str_value = std::static_pointer_cast<RedisString>(obj->value());
    std::string str = str_value->Value();
    cmd->ReplyBulk(std::move(str));
  } else if (obj->type() == TypeInteger) {
    auto int_value = std::static_pointer_cast<RedisInteger>(obj->value());
    std::string str = Int64ToString(int_value->Value());
    cmd->ReplyBulk(std::move(str));
  }
}

bool ObjToString(std::shared_ptr<RedisObj> obj, std::string &v) {
  if (obj->type() & TypeString) {
    auto str_value = std::static_pointer_cast<RedisString>(obj->value());
    v = str_value->Value();
    return true;
  } else if (obj->type() & TypeInteger) {
    auto int_value = std::static_pointer_cast<RedisInteger>(obj->value());
    v = Int64ToString(int_value->Value());
    return true;
  }
  return false;
}

bool ObjToInt64(std::shared_ptr<RedisObj> obj, int64_t &v) {
  if (obj->type() & TypeString) {
    auto str_value = std::static_pointer_cast<RedisString>(obj->value());
    if (StringToInt64(str_value->Value().c_str(), str_value->Value().length(),
                      &v) == 0) {
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
    if (StringToDouble(str_value->Value().c_str(), str_value->Value().length(),
                       &v) == 0) {
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