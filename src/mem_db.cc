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
  }

  expire_list_ = new SkipList<MemObj, EXPIRE_SKIPLIST_LEVEL>(
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
}

MemDB::~MemDB() {}

DicTable<MemObj>::Node *MemDB::GetNode(int dbindex, MemPtr key) {
  auto db = dics_[(dbindex > 0 && dbindex < DBNum) ? dbindex : 0];
  auto tmpkey = std::make_shared<MemObj>(key);
  DicTable<MemObj>::Node *node = db->Get(tmpkey);
  if (node != nullptr && node->data != nullptr && node->data->expire > 0 &&
      node->data->expire < GetMilliSec()) {
    expire_list_->Delete(node->data);
    db->Delete(tmpkey);
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
  auto dic = dics_[(dbindex > 0 && dbindex < DBNum) ? dbindex : 0];
  DicTable<MemObj>::Node *node = new DicTable<MemObj>::Node;
  node->data = obj;
  node->next = nullptr;
  dic->Insert(node);
  if (obj->expire > 0) expire_list_->Insert(obj);
}

void MemDB::UpdateExpire(std::shared_ptr<MemObj> obj, uint64_t expire_ms) {
  if (obj == nullptr || obj->key == nullptr || obj->expire == expire_ms) return;
  if (obj->expire > 0) expire_list_->Delete(obj);
  obj->expire = expire_ms;
  if (expire_ms > 0) expire_list_->Insert(obj);

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
  auto dic = dics_[(dbindex > 0 && dbindex < DBNum) ? dbindex : 0];
  auto obj = Get(dbindex, key);
  if (obj == nullptr) return false;
  if (obj->expire > 0) expire_list_->Delete(obj);
  return dic->Delete(std::make_shared<MemObj>(key));
}

void MemDB::FlushDB(int dbindex) {
  if (dbindex < 0 || dbindex >= DBNum) {
    return;
  }

  dics_[dbindex] =
      std::make_shared<DicTable<MemObj>>(obj_key_equal, obj_key_hash);
}

void MemDB::RehashTimer(uint64_t time) {
  while (DoRehash()) {
    if (GetMilliSec() - time >= 1) break;
  }
}

void MemDB::ExpireTimer(uint64_t time) {}

bool MemDB::DoRehash() {
  bool is_rehash = false;
  for (int j = 0; j < 100; j++) {
    for (size_t i = 0; i < dics_.size(); i++) {
      if (!dics_[i]->RehashStep()) break;
      is_rehash = true;
    }
  }

  return is_rehash;
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