#pragma once
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>
#include <memory>
#include <type_traits>

namespace rockin {
struct membuf_t {
  char *data;
  size_t len;

  membuf_t() : data(nullptr), len(0) {}

  membuf_t(size_t l) : len(l) {
    //..
    data = (char *)malloc(l);
  }

  membuf_t(const std::string &str) {
    len = str.length();
    data = (char *)malloc(len);
    memcpy(data, str.c_str(), len);
  }

  membuf_t(std::string &&str) {
    len = str.length();
    data = (char *)malloc(len);
    memcpy(data, str.c_str(), len);
  }

  membuf_t(char *d, size_t l) : len(l) {
    data = (char *)malloc(l);
    memcpy(data, d, l);
  }

  membuf_t(std::shared_ptr<membuf_t> buf) : len(buf->len) {
    data = (char *)malloc(buf->len);
    memcpy(data, buf->data, buf->len);
  }

  membuf_t(size_t l, std::shared_ptr<membuf_t> buf) : len(l) {
    data = (char *)malloc(l);
    memcpy(data, buf->data, l > buf->len ? buf->len : l);
  }

  ~membuf_t() {
    if (data != nullptr) free(data);
  }

  size_t Size() { return sizeof(membuf_t) + len; }

  bool operator==(const membuf_t &b) const {
    if (len != b.len) return false;
    for (size_t i = 0; i < len; i++) {
      if (*((char *)data + i) != *((char *)b.data + i)) {
        return false;
      }
    }
    return true;
  }
};

extern void change_size(size_t size);

typedef std::shared_ptr<membuf_t> MemPtr;

template <typename _Tp, typename... _Args>
inline _LIBCPP_INLINE_VISIBILITY
    typename std::enable_if<!std::is_array<_Tp>::value,
                            std::shared_ptr<_Tp> >::type
    make_shared(_Args &&... __args) {
  std::shared_ptr<_Tp> ptr(new _Tp(_VSTD::forward<_Args>(__args)...),
                           [](_Tp *ptr) {
                             change_size(0 - ptr->Size());
                             delete ptr;
                           });

  change_size(ptr->Size());
  return ptr;
}

}  // namespace rockin