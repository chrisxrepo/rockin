#include "mem_db.h"
#include "cmd_args.h"
#include "rockin_conn.h"
#include "type_common.h"
#include "type_string.h"

namespace rockin {

MemDB::MemDB() {
  uv_rwlock_init(&rw_lock_);

  for (int i = 0; i < DBNum; i++) {
    dics_.push_back(std::make_shared<RedisDic<MemObj>>());
  }
}

MemDB::~MemDB() {}

std::shared_ptr<MemObj> MemDB::Get(int dbindex, MemPtr key) {
  auto dic = dics_[(dbindex > 0 && dbindex < DBNum) ? dbindex : 0];
  uv_rwlock_rdlock(&rw_lock_);
  auto obj = dic->Get(key);
  uv_rwlock_rdunlock(&rw_lock_);

  return obj;
}

void MemDB::Insert(int dbindex, std::shared_ptr<MemObj> obj) {
  auto dic = dics_[(dbindex > 0 && dbindex < DBNum) ? dbindex : 0];
  uv_rwlock_wrlock(&rw_lock_);
  dic->Insert(obj);
  uv_rwlock_wrunlock(&rw_lock_);
}

// delete by key
bool MemDB::Delete(int dbindex, MemPtr key) {
  auto dic = dics_[(dbindex > 0 && dbindex < DBNum) ? dbindex : 0];
  uv_rwlock_wrlock(&rw_lock_);
  return dic->Delete(key);
  uv_rwlock_wrunlock(&rw_lock_);
}

void MemDB::FlushDB(int dbindex) {
  if (dbindex < 0 || dbindex >= DBNum) {
    return;
  }

  dics_[dbindex] = std::make_shared<RedisDic<MemObj>>();
}

MemPtr GenString(MemPtr value, int encode) {
  if (value != nullptr && encode == Encode_Int) {
    return rockin::make_shared<membuf_t>(Int64ToString(BUF_INT64(value)));
  }

  return value;
}

bool GenInt64(MemPtr str, int encode, int64_t &v) {
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

bool CheckAndReply(std::shared_ptr<MemObj> obj,
                   std::shared_ptr<RockinConn> conn, int type) {
  if (obj == nullptr) {
    conn->ReplyNil();
    return false;
  }

  if (obj->type == type) {
    if (type == Type_String &&
        (obj->encode == Encode_Raw || obj->encode == Encode_Int)) {
      return true;
    }
  }

  static MemPtr g_reply_type_warn = rockin::make_shared<membuf_t>(
      "WRONGTYPE Operation against a key holding the wrong kind of value");

  conn->ReplyError(g_reply_type_warn);
  return false;
}

}  // namespace rockin