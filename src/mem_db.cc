#include "mem_db.h"
#include "cmd_args.h"
#include "rockin_conn.h"
#include "type_common.h"
#include "type_string.h"

namespace rockin {
static SipHash *g_db_hash = nullptr;

static std::shared_ptr<DicTable<MemObj>> make_dic() {
  auto db = std::make_shared<DicTable<MemObj>>(
      [](std::shared_ptr<MemObj> a, std::shared_ptr<MemObj> b) {
        if (a == nullptr || a->key == nullptr || b == nullptr ||
            b->key == nullptr)
          return false;

        if (a->key->len != b->key->len) return false;
        for (size_t i = 0; i < a->key->len; i++) {
          if (*((char *)(a->key->data) + i) != *((char *)(b->key->data) + i)) {
            return false;
          }
        }
        return true;
      },
      [](std::shared_ptr<MemObj> key) {
        return g_db_hash->Hash((const uint8_t *)key->key->data, key->key->len);
      });

  return db;
}

MemDB::MemDB() {
  if (g_db_hash == nullptr) g_db_hash = new SipHash();

  for (int i = 0; i < DBNum; i++) {
    dics_.push_back(make_dic());
  }
}

MemDB::~MemDB() {}

DicTable<MemObj>::Node *MemDB::GetNode(int dbindex, MemPtr key) {
  auto db = dics_[(dbindex > 0 && dbindex < DBNum) ? dbindex : 0];
  auto tmpkey = std::make_shared<MemObj>(key);
  DicTable<MemObj>::Node *node = db->Get(tmpkey);
  if (node != nullptr && node->data != nullptr && node->data->expire > 0 &&
      node->data->expire < GetMilliSec()) {
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
}

void MemDB::Update(int dbindex, std::shared_ptr<MemObj> obj, uint64_t expire) {
  // obj->expire = expire;
}

// delete by key
bool MemDB::Delete(int dbindex, MemPtr key) {
  auto dic = dics_[(dbindex > 0 && dbindex < DBNum) ? dbindex : 0];
  return dic->Delete(std::make_shared<MemObj>(key));
}

void MemDB::FlushDB(int dbindex) {
  if (dbindex < 0 || dbindex >= DBNum) {
    return;
  }

  dics_[dbindex] = make_dic();
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