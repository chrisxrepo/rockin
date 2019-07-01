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
    obj = std::make_shared<RedisObj>();
    obj->key = key;
    obj->next = nullptr;
    dic_.Insert(obj);
  }

  OBJ_SET_VALUE(obj, value, type, encode);
  return obj;
}

// delete by key
bool RedisDB::Delete(std::shared_ptr<buffer_t> key) { return dic_.Delete(key); }

std::shared_ptr<buffer_t> GenString(std::shared_ptr<buffer_t> value,
                                    int encode) {
  if (value != nullptr && encode == Encode_Int) {
    return make_buffer(Int64ToString(BUF_INT64(value)));
  }

  return value;
}

bool GenInt64(std::shared_ptr<buffer_t> str, int encode, int64_t &v) {
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

bool CheckAndReply(std::shared_ptr<RedisObj> obj, std::shared_ptr<RedisCmd> cmd,
                   int type) {
  if (obj->type == type) {
    if (type == Type_String &&
        (obj->encode == Encode_Raw || obj->encode == Encode_Int)) {
      return true;
    }
  }

  cmd->ReplyError(RedisCmd::g_reply_type_warn);
  return false;
}

void ReplyRedisObj(std::shared_ptr<RedisObj> obj,
                   std::shared_ptr<RedisCmd> cmd) {
  if (obj == nullptr) {
    cmd->ReplyNil();
  } else if (obj->type == Type_String && obj->encode == Encode_Raw) {
    auto str_value = std::static_pointer_cast<buffer_t>(obj->value);
    cmd->ReplyBulk(str_value);
  } else if (obj->type == Type_String && obj->encode == Encode_Int) {
    auto str_value = std::static_pointer_cast<buffer_t>(obj->value);
    cmd->ReplyInteger(BUF_INT64(str_value));
  }
}

}  // namespace rockin