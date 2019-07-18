#pragma once
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>
#include <memory>
#include <type_traits>
#include <vector>

namespace rockin {

// change used memory size
extern void change_momory_size(size_t size);

// get used memory size
extern uint64_t get_memory_size();

struct buffer_t {
  char *data;
  size_t len;
  bool alloc;

  buffer_t() : data(nullptr), len(0), alloc(false) {}

  buffer_t(size_t l) : len(l), alloc(true) {
    //..
    data = (char *)malloc(l);
  }

  buffer_t(const std::string &str) : alloc(true) {
    len = str.length();
    data = (char *)malloc(len);
    memcpy(data, str.c_str(), len);
  }

  buffer_t(std::string &&str) : alloc(true) {
    len = str.length();
    data = (char *)malloc(len);
    memcpy(data, str.c_str(), len);
  }

  buffer_t(char *d, size_t l) : len(l), alloc(true) {
    data = (char *)malloc(l);
    memcpy(data, d, l);
  }

  buffer_t(std::shared_ptr<buffer_t> buf) : len(buf->len), alloc(true) {
    data = (char *)malloc(buf->len);
    memcpy(data, buf->data, buf->len);
  }

  buffer_t(size_t l, std::shared_ptr<buffer_t> buf) : len(l), alloc(true) {
    data = (char *)malloc(l);
    memcpy(data, buf->data, l > buf->len ? buf->len : l);
  }

  ~buffer_t() {
    if (alloc && data != nullptr) free(data);
  }

  bool operator==(const buffer_t &b) const {
    if (len != b.len) return false;
    for (size_t i = 0; i < len; i++) {
      if (*((char *)data + i) != *((char *)b.data + i)) {
        return false;
      }
    }
    return true;
  }
};

typedef std::shared_ptr<buffer_t> BufPtr;
typedef std::vector<BufPtr> BufPtrs;
typedef std::vector<std::pair<BufPtr, BufPtr>> KVPairS;

template <typename... _Args>
std::shared_ptr<buffer_t> make_buffer(_Args &&... __args) {
  std::shared_ptr<buffer_t> ptr(
      new buffer_t(std::forward<_Args>(__args)...), [](buffer_t *ptr) {
        change_momory_size(0 - (sizeof(buffer_t) + ptr->len));
        delete ptr;
      });

  change_momory_size((sizeof(buffer_t) + ptr->len));
  return ptr;
}

////////////////////////////////////////////////////////////////////
enum ValueType {
  Type_None = 0,
  Type_String = 1,
  Type_List = 2,
  Type_Hash = 4,
  Type_Set = 8,
  Type_ZSet = 16,
};

enum EncodeType {
  Encode_None = 0,
  Encode_Raw = 1,
  Encode_Int = 2,
};

struct object_t {
  uint8_t type;
  uint8_t encode;
  uint32_t version;
  uint64_t expire;
  BufPtr key;
  std::shared_ptr<void> value;
  std::shared_ptr<object_t> next;

  object_t()
      : type(0),
        encode(0),
        version(0),
        expire(0),
        key(nullptr),
        value(nullptr),
        next(nullptr) {}

  object_t(BufPtr key_)
      : type(0),
        encode(0),
        version(0),
        expire(0),
        key(key_),
        value(nullptr),
        next(nullptr) {}

  size_t Size() { return sizeof(object_t); }
};

typedef std::shared_ptr<object_t> ObjPtr;
typedef std::vector<ObjPtr> ObjPtrs;

template <typename... _Args>
ObjPtr make_object(_Args &&... __args) {
  ObjPtr ptr(new object_t(std::forward<_Args>(__args)...), [](object_t *ptr) {
    change_momory_size(0 - sizeof(buffer_t));

    delete ptr;
  });

  change_momory_size(sizeof(buffer_t));
  return ptr;
}

#define OBJ_SET_VALUE(o, v, t, e) \
  do {                            \
    (o)->type = (t);              \
    (o)->encode = (e);            \
    (o)->value = (v);             \
  } while (0)

}  // namespace rockin