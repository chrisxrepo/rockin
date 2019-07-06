#include "mem_db.h"
#include "cmd_args.h"
#include "rockin_conn.h"
#include "type_common.h"
#include "type_string.h"

namespace rockin {

MemDB::MemDB() {
  for (int i = 0; i < DBNum; i++) {
    dics_.push_back(std::make_shared<RedisDic<RedisObj>>());
  }
}

MemDB::~MemDB() {}

std::shared_ptr<RedisObj> MemDB::Get(int dbindex,
                                     std::shared_ptr<buffer_t> key) {
  auto dic = dics_[(dbindex > 0 && dbindex < DBNum) ? dbindex : 0];
  return dic->Get(key);
}

std::shared_ptr<RedisObj> MemDB::GetReplyNil(int dbindex,
                                             std::shared_ptr<buffer_t> key,
                                             std::shared_ptr<RockinConn> conn) {
  auto obj = Get(dbindex, key);
  if (obj == nullptr) {
    conn->ReplyNil();
    return nullptr;
  }

  return obj;
}

std::shared_ptr<RedisObj> MemDB::Set(int dbindex, std::shared_ptr<buffer_t> key,
                                     std::shared_ptr<void> value,
                                     unsigned char type, unsigned char encode) {
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
bool MemDB::Delete(int dbindex, std::shared_ptr<buffer_t> key) {
  auto dic = dics_[(dbindex > 0 && dbindex < DBNum) ? dbindex : 0];
  return dic->Delete(key);
}

void MemDB::FlushDB(int dbindex) {
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

bool CheckAndReply(std::shared_ptr<RedisObj> obj,
                   std::shared_ptr<RockinConn> conn, int type) {
  if (obj->type == type) {
    if (type == Type_String &&
        (obj->encode == Encode_Raw || obj->encode == Encode_Int)) {
      return true;
    }
  }

  static std::shared_ptr<buffer_t> g_reply_type_warn = make_buffer(
      "WRONGTYPE Operation against a key holding the wrong kind of value");

  conn->ReplyError(g_reply_type_warn);
  return false;
}

void ReplyRedisObj(std::shared_ptr<RedisObj> obj,
                   std::shared_ptr<RockinConn> conn) {
  if (obj == nullptr) {
    conn->ReplyNil();
  } else if (obj->type == Type_String && obj->encode == Encode_Raw) {
    auto str_value = std::static_pointer_cast<buffer_t>(obj->value);
    conn->ReplyBulk(str_value);
  } else if (obj->type == Type_String && obj->encode == Encode_Int) {
    auto str_value = std::static_pointer_cast<buffer_t>(obj->value);
    conn->ReplyInteger(BUF_INT64(str_value));
  }
}

}  // namespace rockin