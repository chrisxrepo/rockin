#include "redis_db.h"
#include "redis_cmd.h"
#include "redis_common.h"
#include "redis_string.h"

namespace rockin {

RedisDB::RedisDB() {
  for (int i = 0; i < DBNum; i++) {
    dics_.push_back(std::make_shared<RedisDic<RedisObj>>());
  }
}

RedisDB::~RedisDB() {}

std::shared_ptr<RedisObj> RedisDB::Get(int dbindex,
                                       std::shared_ptr<buffer_t> key) {
  auto dic = dics_[(dbindex > 0 && dbindex < DBNum) ? dbindex : 0];
  return dic->Get(key);
}

std::shared_ptr<RedisObj> RedisDB::GetReplyNil(int dbindex,
                                               std::shared_ptr<buffer_t> key,
                                               std::shared_ptr<RedisCmd> cmd) {
  auto obj = Get(dbindex, key);
  if (obj == nullptr) {
    cmd->ReplyNil();
    return nullptr;
  }

  return obj;
}

std::shared_ptr<RedisObj> RedisDB::Set(int dbindex,
                                       std::shared_ptr<buffer_t> key,
                                       std::shared_ptr<void> value,
                                       unsigned char type,
                                       unsigned char encode) {
  auto dic = dics_[(dbindex > 0 && dbindex < DBNum) ? dbindex : 0];
  auto obj = dic->Get(key);
  if (obj == nullptr) {
    obj = std::make_shared<RedisObj>();
    obj->key = key;
    obj->next = nullptr;
    dic->Insert(obj);
  }

  OBJ_SET_VALUE(obj, value, type, encode);
  return obj;
}

// delete by key
bool RedisDB::Delete(int dbindex, std::shared_ptr<buffer_t> key) {
  auto dic = dics_[(dbindex > 0 && dbindex < DBNum) ? dbindex : 0];
  return dic->Delete(key);
}

void RedisDB::FlushDB(int dbindex) {
  if (dbindex < 0 || dbindex >= DBNum) {
    return;
  }

  dics_[dbindex] = std::make_shared<RedisDic<RedisObj>>();
}

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