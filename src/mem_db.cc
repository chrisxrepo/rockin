#include "mem_db.h"
#include "cmd_args.h"
#include "rockin_conn.h"
#include "type_common.h"
#include "type_string.h"

namespace rockin {
static SipHash *g_db_hash = nullptr;

static uint64_t obj_key_hash(std::shared_ptr<MemObj> obj) {
  return g_db_hash->Hash((const uint8_t *)obj->key->data, obj->key->len);
}

static bool obj_key_equal(std::shared_ptr<MemObj> a,
                          std::shared_ptr<MemObj> b) {
  if ((a == nullptr || a->key == nullptr) &&
      (b == nullptr || b->key == nullptr))
    return true;

  if (a == nullptr || a->key == nullptr || b == nullptr || b->key == nullptr)
    return false;

  if (a->key->len != b->key->len) return false;
  for (size_t i = 0; i < a->key->len; i++) {
    if (*((char *)(a->key->data) + i) != *((char *)(b->key->data) + i)) {
      return false;
    }
  }
  return true;
}

static int obj_key_compare(std::shared_ptr<MemObj> a,
                           std::shared_ptr<MemObj> b) {
  if ((a == nullptr || a->key == nullptr) &&
      (b == nullptr || b->key == nullptr))
    return 0;
  if (a == nullptr || a->key == nullptr) return -1;
  if (b == nullptr || b->key == nullptr) return 1;

  for (size_t i = 0; i < a->key->len && i < b->key->len; i++) {
    uint8_t ac = *((uint8_t *)(a->key->data) + i);
    uint8_t bc = *((uint8_t *)(b->key->data) + i);
    if (ac > bc) return 1;
    if (ac < bc) return -1;
  }

  if (a->key->len > b->key->len) return 1;
  if (a->key->len < b->key->len) return -1;
  return 0;
}

MemDB::MemDB() {
  if (g_db_hash == nullptr) g_db_hash = new SipHash();

  for (int i = 0; i < DBNum; i++) {
    dics_.push_back(
        std::make_shared<DicTable<MemObj>>(obj_key_equal, obj_key_hash));

    auto expire = std::make_shared<SkipList<MemObj, EXPIRE_SKIPLIST_LEVEL>>(
        [](std::shared_ptr<MemObj> a, std::shared_ptr<MemObj> b) {
          if (a == nullptr && b == nullptr) return 0;
          if (a == nullptr) return -1;
          if (b == nullptr) return 1;
          if (a->expire > b->expire) return 1;
          if (a->expire < b->expire) return -1;
          return obj_key_compare(a, b);
        },
        [](std::shared_ptr<MemObj> a, std::shared_ptr<MemObj> b) {
          if (a == nullptr && b == nullptr) return true;
          if (a == nullptr || b == nullptr) return false;
          if (a->expire != b->expire) return false;
          return obj_key_equal(a, b);
        });

    expires_.push_back(expire);
  }
}

MemDB::~MemDB() {}

DicTable<MemObj>::Node *MemDB::GetNode(int dbindex, MemPtr key) {
  int idx = (dbindex > 0 && dbindex < DBNum) ? dbindex : 0;
  auto tmpkey = std::make_shared<MemObj>(key);
  DicTable<MemObj>::Node *node = dics_[idx]->Get(tmpkey);
  if (node != nullptr && node->data != nullptr && node->data->expire > 0 &&
      node->data->expire < GetMilliSec()) {
    expires_[idx]->Delete(node->data);
    dics_[idx]->Delete(tmpkey);
    return nullptr;
  }
  return node;
}

std::shared_ptr<MemObj> MemDB::Get(int dbindex, MemPtr key) {
  DicTable<MemObj>::Node *node = GetNode(dbindex, key);
  if (node == nullptr) return nullptr;
  return node->data;
}

void MemDB::Insert(int dbindex, std::shared_ptr<MemObj> obj) {
  int idx = (dbindex > 0 && dbindex < DBNum) ? dbindex : 0;
  DicTable<MemObj>::Node *node = new DicTable<MemObj>::Node;
  node->data = obj;
  node->next = nullptr;
  dics_[idx]->Insert(node);
  if (obj->expire > 0) expires_[idx]->Insert(obj);
}

void MemDB::UpdateExpire(int dbindex, std::shared_ptr<MemObj> obj,
                         uint64_t expire_ms) {
  if (obj == nullptr || obj->key == nullptr || obj->expire == expire_ms) return;
  int idx = (dbindex > 0 && dbindex < DBNum) ? dbindex : 0;
  if (obj->expire > 0) expires_[idx]->Delete(obj);
  obj->expire = expire_ms;
  if (expire_ms > 0) expires_[idx]->Insert(obj);

  /*  std::cout << "Expire List:";
   expire_list_->Range([](std::shared_ptr<MemObj> obj) {
     std::cout << std::string(obj->key->data, obj->key->len) << ":"
               << obj->expire << " ";
     return true;
   });
   std::cout << std::endl;
   */
}

// delete by key
bool MemDB::Delete(int dbindex, MemPtr key) {
  int idx = (dbindex > 0 && dbindex < DBNum) ? dbindex : 0;
  auto obj = Get(dbindex, key);
  if (obj == nullptr) return false;
  if (obj->expire > 0) expires_[idx]->Delete(obj);
  return dics_[idx]->Delete(std::make_shared<MemObj>(key));
}

void MemDB::FlushDB(int dbindex) {
  if (dbindex < 0 || dbindex >= DBNum) {
    return;
  }

  dics_[dbindex] =
      std::make_shared<DicTable<MemObj>>(obj_key_equal, obj_key_hash);
}

void MemDB::RehashTimer(uint64_t time) {
  bool is_rehash = false;
  do {
    is_rehash = false;
    for (int j = 0; j < 100; j++) {
      for (size_t i = 0; i < dics_.size(); i++) {
        if (!dics_[i]->RehashStep()) break;
        is_rehash = true;
      }
    }

    if (GetMilliSec() - time >= 1) break;
  } while (is_rehash);
}

void MemDB::ExpireTimer(uint64_t time) {
  bool is_expire = false;
  do {
    is_expire = false;
    uint64_t cur_ms = GetMilliSec();
    for (int i = 0; i < expires_.size(); i++) {
      int expire_count = 0;
      std::vector<std::shared_ptr<MemObj>> expire_objs;
      expires_[i]->Range([&is_expire, &expire_count, &expire_objs,
                          cur_ms](std::shared_ptr<MemObj> obj) {
        if (cur_ms >= obj->expire) {
          is_expire = true;
          expire_objs.push_back(obj);
          if (++expire_count > 100) return false;
          return true;
        }
        return false;
      });

      for (auto iter = expire_objs.begin(); iter != expire_objs.end(); ++iter) {
        dics_[i]->Delete(*iter);
        expires_[i]->Delete(*iter);
      }

      if (expire_count > 0)
        std::cout << "db" << i << ": remove expire obj " << expire_objs.size()
                  << std::endl;
    }

    if (GetMilliSec() - time >= 1) break;
  } while (is_expire);
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