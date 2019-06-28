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

std::shared_ptr<RedisObj> RedisDB::Set(std::shared_ptr<buffer_t> key,
                                       std::shared_ptr<void> value,
                                       unsigned char type,
                                       unsigned char encode) {
  auto obj = dic_.Get(key);
  if (obj == nullptr) {
    obj = std::make_shared<RedisObj>(type, encode, std::move(key), value);
    dic_.Insert(obj);
  } else {
    obj->type_ = type;
    obj->encode_ = encode;
    obj->value_ = value;
  }
  return obj;
}

void RedisDB::SetObj(std::shared_ptr<RedisObj> obj, std::shared_ptr<void> value,
                     unsigned char type, unsigned char encode) {
  obj->value_ = value;
  obj->type_ = type;
  obj->encode_ = encode;
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
  } else if (obj->type() == RedisObj::String) {
    auto str_value = std::static_pointer_cast<RedisString>(obj->value());
    if (obj->encode() == RedisString::Int) {
      cmd->ReplyInteger(str_value->IntValue());
    } else {
      cmd->ReplyBulk(str_value->Value());
    }
  }
}

}  // namespace rockin