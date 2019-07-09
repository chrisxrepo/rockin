#pragma once
#include <stdint.h>

namespace rockin {
inline void EncodeFixed8(char* buf, uint8_t value) {
  //...
  buf[0] = value & 0xff;
}

inline void EncodeFixed16(char* buf, uint16_t value) {
  buf[0] = value & 0xff;
  buf[1] = (value >> 8) & 0xff;
}

inline void EncodeFixed32(char* buf, uint32_t value) {
  buf[0] = value & 0xff;
  buf[1] = (value >> 8) & 0xff;
  buf[2] = (value >> 16) & 0xff;
  buf[3] = (value >> 24) & 0xff;
}

inline void EncodeFixed64(char* buf, uint64_t value) {
  buf[0] = value & 0xff;
  buf[1] = (value >> 8) & 0xff;
  buf[2] = (value >> 16) & 0xff;
  buf[3] = (value >> 24) & 0xff;
  buf[4] = (value >> 32) & 0xff;
  buf[5] = (value >> 40) & 0xff;
  buf[6] = (value >> 48) & 0xff;
  buf[7] = (value >> 56) & 0xff;
}

inline uint8_t DecodeFixed8(const char* ptr) {
  return static_cast<uint8_t>(static_cast<unsigned char>(ptr[0]));
}

inline uint16_t DecodeFixed16(const char* ptr) {
  return ((static_cast<uint16_t>(static_cast<unsigned char>(ptr[0]))) |
          (static_cast<uint16_t>(static_cast<unsigned char>(ptr[1])) << 8));
}

inline uint32_t DecodeFixed32(const char* ptr) {
  uint32_t lo = DecodeFixed16(ptr);
  uint32_t hi = DecodeFixed16(ptr + 2);
  return (hi << 16) | lo;
}

inline uint64_t DecodeFixed64(const char* ptr) {
  uint64_t lo = DecodeFixed32(ptr);
  uint64_t hi = DecodeFixed32(ptr + 4);
  return (hi << 32) | lo;
}

}  // namespace rockin