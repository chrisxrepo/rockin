#pragma once
#include <stdint.h>

namespace rockin {
class SipHash {
 public:
  SipHash();
  SipHash(uint8_t seed[16]);

  uint64_t Hash(const uint8_t *in, int inlen);

 private:
  uint8_t seed_[16];
};
}  // namespace rockin