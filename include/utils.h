#pragma once
#include <uv.h>
#include <atomic>
#include <cstdio>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#define FUN_BIND0(ClassType, ObjPtr, MemFun) \
  std::bind(&ClassType::MemFun, ObjPtr)

#define FUN_BIND1(ClassType, ObjPtr, MemFun) \
  std::bind(&ClassType::MemFun, ObjPtr, std::placeholders::_1)

#define FUN_BIND2(ClassType, ObjPtr, MemFun)                   \
  std::bind(&ClassType::MemFun, ObjPtr, std::placeholders::_1, \
            std::placeholders::_2)

#define FUN_BIND3(ClassType, ObjPtr, MemFun)                   \
  std::bind(&ClassType::MemFun, ObjPtr, std::placeholders::_1, \
            std::placeholders::_2, std::placeholders::_3)

#define FUN_STATIC_BIND0(MemFun) std::bind(&MemFun)

#define FUN_STATIC_BIND1(MemFun) std::bind(&MemFun, std::placeholders::_1)

#define FUN_STATIC_BIND2(MemFun) \
  std::bind(&MemFun, std::placeholders::_1, std::placeholders::_2)

#define FUN_STATIC_BIND3(MemFun)                                   \
  std::bind(&MemFun, std::placeholders::_1, std::placeholders::_2, \
            std::placeholders::_3)

#define container_of(ptr, type, member) \
  ((type *)((char *)(ptr)-offsetof(type, member)))

namespace rockin {

// get ctype error
extern std::string GetCerr();

// get libuv error by errcode
extern std::string GetUvError(int errcode);

// get millisecond time
extern uint64_t GetMilliSec();

// set socket nonblocking
extern bool SetNonBlocking(int sock);

// set reuse address
extern bool SetReuseAddr(int sock);

// set reuse port -  >= linux 3.9
extern bool SetReusePort(int sock);

// set close-on-exec
extern bool SetCloseOnExec(int sock);

// set tcp server keep alive
extern bool SetKeepAlive(int sock);

// set socket no delay
extern bool SetNoDelay(int sock);

inline char *Strchr(char *s, size_t l, char c) {
  for (size_t i = 0; i < l; ++i) {
    if (s[i] == c) return s + i;
  }
  return nullptr;
}

inline char *Strchr2(char *s, size_t l, char c1, char c2) {
  for (size_t i = 1; i < l; ++i) {
    if (s[i - 1] == c1 && s[i] == c2) return s + (i - 1);
  }
  return nullptr;
}

// convert int64_t to string
extern int Int64ToString(char *s, size_t len, int64_t value);
extern std::string Int64ToString(int64_t value);

// convert strint to int64_t
extern int StringToInt64(const char *s, size_t slen, int64_t *value);
extern int64_t StringToInt64(const char *s, size_t slen);

// char <-> hex
extern bool IsHexDigit(char c);
extern char HexDigitToInt(char c);

// get next power of 2
extern uint32_t NextPower(uint32_t size);

// get random bytes
extern void RandomBytes(unsigned char bytes[], size_t len);

// simple hashcode
extern uint32_t SimpleHash(const char *src, size_t len);

// bit 1 count
extern size_t BitCount(void *s, long count);
// first bit
extern long Bitpos(void *s, unsigned long count, int bit);

// get directory size
extern int64_t GetDirectorySize(const std::string &dir);

// get size string
// B/K/M/G/T
extern std::string GetSizeString(int64_t size);

// print string by hex mode
extern void PrintHex(const char *data, size_t len);

template <typename... Args>
std::string Format(const std::string &format, Args... args) {
  size_t size = snprintf(nullptr, 0, format.c_str(), args...) +
                1;  // Extra space for '\0'
  std::unique_ptr<char[]> buf(new char[size]);
  snprintf(buf.get(), size, format.c_str(), args...);
  return std::string(buf.get(),
                     buf.get() + size - 1);  // We don't want the '\0' inside
}

}  // namespace rockin