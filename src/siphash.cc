#include "siphash.h"
#include <string.h>
#include <mutex>

#if defined(__X86_64__) || defined(__x86_64__) || defined(__i386__)
#define UNALIGNED_LE_CPU
#endif

#ifdef UNALIGNED_LE_CPU
#define U8TO64_LE(p) (*((uint64_t *)(p)))
#else
#define U8TO64_LE(p)                                         \
  (((uint64_t)((p)[0])) | ((uint64_t)((p)[1]) << 8) |        \
   ((uint64_t)((p)[2]) << 16) | ((uint64_t)((p)[3]) << 24) | \
   ((uint64_t)((p)[4]) << 32) | ((uint64_t)((p)[5]) << 40) | \
   ((uint64_t)((p)[6]) << 48) | ((uint64_t)((p)[7]) << 56))
#endif

#define ROTL(x, b) (uint64_t)(((x) << (b)) | ((x) >> (64 - (b))))

#define SIPROUND       \
  do {                 \
    v0 += v1;          \
    v1 = ROTL(v1, 13); \
    v1 ^= v0;          \
    v0 = ROTL(v0, 32); \
    v2 += v3;          \
    v3 = ROTL(v3, 16); \
    v3 ^= v2;          \
    v0 += v3;          \
    v3 = ROTL(v3, 21); \
    v3 ^= v0;          \
    v2 += v1;          \
    v1 = ROTL(v1, 17); \
    v1 ^= v2;          \
    v2 = ROTL(v2, 32); \
  } while (0)

namespace rockin {
SipHash::SipHash() { memset(seed_, 0, 16); }

SipHash::SipHash(uint8_t seed[16]) { memcpy(seed_, seed, 16); }

uint64_t SipHash::Hash(const uint8_t *in, int inlen) {
#ifndef UNALIGNED_LE_CPU
  uint64_t hash;
  uint8_t *out = (uint8_t *)&hash;
#endif
  uint64_t v0 = 0x736f6d6570736575ULL;
  uint64_t v1 = 0x646f72616e646f6dULL;
  uint64_t v2 = 0x6c7967656e657261ULL;
  uint64_t v3 = 0x7465646279746573ULL;
  uint64_t k0 = U8TO64_LE(seed_);
  uint64_t k1 = U8TO64_LE(seed_ + 8);
  uint64_t m;
  const uint8_t *end = in + inlen - (inlen % sizeof(uint64_t));
  const int left = inlen & 7;
  uint64_t b = ((uint64_t)inlen) << 56;
  v3 ^= k1;
  v2 ^= k0;
  v1 ^= k1;
  v0 ^= k0;

  for (; in != end; in += 8) {
    m = U8TO64_LE(in);
    v3 ^= m;

    SIPROUND;

    v0 ^= m;
  }

  switch (left) {
    case 7:
      b |= ((uint64_t)in[6]) << 48; /* fall-thru */
    case 6:
      b |= ((uint64_t)in[5]) << 40; /* fall-thru */
    case 5:
      b |= ((uint64_t)in[4]) << 32; /* fall-thru */
    case 4:
      b |= ((uint64_t)in[3]) << 24; /* fall-thru */
    case 3:
      b |= ((uint64_t)in[2]) << 16; /* fall-thru */
    case 2:
      b |= ((uint64_t)in[1]) << 8; /* fall-thru */
    case 1:
      b |= ((uint64_t)in[0]);
      break;
    case 0:
      break;
  }

  v3 ^= b;

  SIPROUND;

  v0 ^= b;
  v2 ^= 0xff;

  SIPROUND;
  SIPROUND;

  b = v0 ^ v1 ^ v2 ^ v3;
#ifndef UNALIGNED_LE_CPU
  U64TO8_LE(out, b);
  return hash;
#else
  return b;
#endif
}

namespace {
std::once_flag once_flag;
SipHash *g_data;
};  // namespace

uint64_t Hash(const void *in, int inlen) {
  std::call_once(once_flag, []() { g_data = new SipHash(); });
  return g_data->Hash((const uint8_t *)in, inlen);
}

}  // namespace rockin