#include "utils.h"
#include <errno.h>
#include <limits.h>
#include <math.h>
#include <stdio.h>
#include <string.h>
#include <random>
#include <sstream>
#include "dirent.h"

namespace rockin {

std::atomic<int64_t> g_buffer_size;

std::shared_ptr<buffer_t> make_buffer(size_t len) {
  char *d = (char *)malloc(len);
  std::shared_ptr<buffer_t> ptr(new buffer_t(d, len), [](buffer_t *buf) {
    g_buffer_size.fetch_sub(buf->len);
    if (buf->data != nullptr) free(buf->data);
    delete buf;
  });

  g_buffer_size.fetch_add(len);
  return ptr;
}

std::shared_ptr<buffer_t> make_buffer(const char *v, size_t len) {
  char *d = (char *)malloc(len);
  std::shared_ptr<buffer_t> ptr(new buffer_t(d, len), [](buffer_t *buf) {
    g_buffer_size.fetch_sub(buf->len);
    if (buf->data != nullptr) free(buf->data);
    delete buf;
  });

  g_buffer_size.fetch_add(len);
  memcpy(ptr->data, v, len);
  return ptr;
}

std::shared_ptr<buffer_t> make_buffer(const std::string &str) {
  return make_buffer(str.c_str(), str.length());
}

std::shared_ptr<buffer_t> copy_buffer(std::shared_ptr<buffer_t> v, size_t len) {
  if (v == nullptr) {
    return make_buffer(len);
  }

  char *d = (char *)malloc(len);
  std::shared_ptr<buffer_t> ptr(new buffer_t(d, len), [](buffer_t *buf) {
    g_buffer_size.fetch_sub(buf->len);
    if (buf->data != nullptr) free(buf->data);
    delete buf;
  });

  g_buffer_size.fetch_add(len);
  memcpy(ptr->data, v->data, len > v->len ? v->len : len);
  return ptr;
}

std::string GetCerr() {
  const char *errstr = strerror(errno);
  if (errstr == nullptr) {
    return "";
  }
  return std::string(errstr);
}

std::string GetUvError(int errcode) {
  std::string err;
  err = uv_err_name(errcode);
  err += ":";
  err += uv_strerror(errcode);
  return std::move(err);
}

bool SetNonBlocking(int sock) {
#ifdef _WIN32
  {
    unsigned long nonblocking = 1;
    if (ioctlsocket(sockt, FIONBIO, &nonblocking) == SOCKET_ERROR) {
      std::cout << "fcntl(" << sock << ", F_GETFL)" << std::endl;
      return false;
    }
  }
#else
  {
    int flags;
    if ((flags = fcntl(sock, F_GETFL, NULL)) < 0) {
      std::cout << "fcntl(" << sock << ", F_GETFL)" << std::endl;
      return false;
    }
    if (!(flags & O_NONBLOCK)) {
      if (fcntl(sock, F_SETFL, flags | O_NONBLOCK) == -1) {
        std::cout << "fcntl(" << sock << ", F_SETFL)" << std::endl;
        return false;
      }
    }
  }
#endif

  return true;
}

bool SetReuseAddr(int sock) {
#if defined(SO_REUSEADDR) && !defined(_WIN32)
  int reuse_addr = 1;
  int ret = setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, (void *)&reuse_addr,
                       sizeof(reuse_addr));
  if (ret != 0) {
    return false;
  }
#endif
  return true;
}

// set reuse port -  >= linux 3.9
bool SetReusePort(int sock) {
#if defined __linux__ && defined(SO_REUSEPORT)
  int reuse_port = 1;
  int ret = setsockopt(sock, SOL_SOCKET, SO_REUSEPORT, (void *)&reuse_port,
                       sizeof(reuse_port));
  if (ret != 0) {
    return false;
  }
#endif
  return true;
}

// set close-on-exec
bool SetCloseOnExec(int sock) {
#if !defined(_WIN32)
  int ret = fcntl(sock, F_GETFD);
  if (ret < 0) {
    return false;
  }

  ret = fcntl(sock, F_SETFD, ret | FD_CLOEXEC);
  if (ret < 0) {
    return false;
  }
#endif
  return true;
}

// set tcp server keep alive
bool SetKeepAlive(int sock) {
  int keep_alive = 1;
  int ret = setsockopt(sock, SOL_SOCKET, SO_KEEPALIVE, (char *)&keep_alive,
                       sizeof(keep_alive));
  if (ret != 0) {
    return false;
  }

  return true;
}

// set socket no delay
bool SetNoDelay(int sock) {
  int no_delay = 1;
  int ret = setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, (char *)&no_delay,
                       sizeof(no_delay));

  if (ret != 0) {
    return false;
  }

  return true;
}

int Int64ToString(char *s, size_t len, int64_t value) {
  char buf[32], *p;
  unsigned long long v;
  size_t l;

  if (len == 0) return 0;
  v = (value < 0) ? -value : value;
  p = buf + 31; /* point to the last character */
  do {
    *p-- = '0' + (v % 10);
    v /= 10;
  } while (v);
  if (value < 0) *p-- = '-';
  p++;
  l = 32 - (p - buf);
  if (l + 1 > len) l = len - 1; /* Make sure it fits, including the nul term */
  memcpy(s, p, l);
  s[l] = '\0';
  return l;
}

std::string Int64ToString(int64_t value) {
  char buf[32];
  int len = Int64ToString(buf, 32, value);
  std::string vstr(buf, len);
  return std::move(vstr);
}

int StringToInt64(const char *s, size_t slen, int64_t *value) {
  const char *p = s;
  size_t plen = 0;
  int negative = 0;
  uint64_t v;

  if (plen == slen) return 0;

  /* Special case: first and only digit is 0. */
  if (slen == 1 && p[0] == '0') {
    if (value != NULL) *value = 0;
    return 1;
  }

  if (p[0] == '-') {
    negative = 1;
    p++;
    plen++;

    /* Abort on only a negative sign. */
    if (plen == slen) return 0;
  }

  /* First digit should be 1-9, otherwise the string should just be 0. */
  if (p[0] >= '1' && p[0] <= '9') {
    v = p[0] - '0';
    p++;
    plen++;
  } else if (p[0] == '0' && slen == 1) {
    *value = 0;
    return 1;
  } else
    return 0;

  while (plen < slen && p[0] >= '0' && p[0] <= '9') {
    if (v > (UINT64_MAX / 10)) /* Overflow. */
      return 0;
    v *= 10;

    if (v > (UINT64_MAX - (p[0] - '0'))) /* Overflow. */
      return 0;
    v += p[0] - '0';

    p++;
    plen++;
  }

  /* Return if not all bytes were used. */
  if (plen < slen) return 0;

  if (negative) {
    if (v > ((uint64_t)(-(INT64_MIN + 1)) + 1)) /* Overflow. */
      return 0;
    if (value != NULL) *value = -v;
  } else {
    if (v > INT64_MAX) /* Overflow. */
      return 0;
    if (value != NULL) *value = v;
  }
  return 1;
}

int64_t StringToInt64(const char *s, size_t slen) {
  int64_t value = 0;
  if (StringToInt64(s, slen, &value) == 0) return 0;
  return std::move(value);
}

size_t BitCount(void *s, long count) {
  size_t bits = 0;
  unsigned char *p = (unsigned char *)s;
  uint32_t *p4;
  static const unsigned char bitsinbyte[256] = {
      0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 1, 2, 2, 3, 2, 3, 3, 4,
      2, 3, 3, 4, 3, 4, 4, 5, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
      2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 1, 2, 2, 3, 2, 3, 3, 4,
      2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
      2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6,
      4, 5, 5, 6, 5, 6, 6, 7, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
      2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5,
      3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
      2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6,
      4, 5, 5, 6, 5, 6, 6, 7, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
      4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8};

  /* Count initial bytes not aligned to 32 bit. */
  while ((unsigned long)p & 3 && count) {
    bits += bitsinbyte[*p++];
    count--;
  }

  /* Count bits 28 bytes at a time */
  p4 = (uint32_t *)p;
  while (count >= 28) {
    uint32_t aux1, aux2, aux3, aux4, aux5, aux6, aux7;

    aux1 = *p4++;
    aux2 = *p4++;
    aux3 = *p4++;
    aux4 = *p4++;
    aux5 = *p4++;
    aux6 = *p4++;
    aux7 = *p4++;
    count -= 28;

    aux1 = aux1 - ((aux1 >> 1) & 0x55555555);
    aux1 = (aux1 & 0x33333333) + ((aux1 >> 2) & 0x33333333);
    aux2 = aux2 - ((aux2 >> 1) & 0x55555555);
    aux2 = (aux2 & 0x33333333) + ((aux2 >> 2) & 0x33333333);
    aux3 = aux3 - ((aux3 >> 1) & 0x55555555);
    aux3 = (aux3 & 0x33333333) + ((aux3 >> 2) & 0x33333333);
    aux4 = aux4 - ((aux4 >> 1) & 0x55555555);
    aux4 = (aux4 & 0x33333333) + ((aux4 >> 2) & 0x33333333);
    aux5 = aux5 - ((aux5 >> 1) & 0x55555555);
    aux5 = (aux5 & 0x33333333) + ((aux5 >> 2) & 0x33333333);
    aux6 = aux6 - ((aux6 >> 1) & 0x55555555);
    aux6 = (aux6 & 0x33333333) + ((aux6 >> 2) & 0x33333333);
    aux7 = aux7 - ((aux7 >> 1) & 0x55555555);
    aux7 = (aux7 & 0x33333333) + ((aux7 >> 2) & 0x33333333);
    bits += ((((aux1 + (aux1 >> 4)) & 0x0F0F0F0F) +
              ((aux2 + (aux2 >> 4)) & 0x0F0F0F0F) +
              ((aux3 + (aux3 >> 4)) & 0x0F0F0F0F) +
              ((aux4 + (aux4 >> 4)) & 0x0F0F0F0F) +
              ((aux5 + (aux5 >> 4)) & 0x0F0F0F0F) +
              ((aux6 + (aux6 >> 4)) & 0x0F0F0F0F) +
              ((aux7 + (aux7 >> 4)) & 0x0F0F0F0F)) *
             0x01010101) >>
            24;
  }
  /* Count the remaining bytes. */
  p = (unsigned char *)p4;
  while (count--) bits += bitsinbyte[*p++];
  return bits;
}

long Bitpos(void *s, unsigned long count, int bit) {
  unsigned long *l;
  unsigned char *c;
  unsigned long skipval, word = 0, one;
  long pos = 0; /* Position of bit, to return to the caller. */
  unsigned long j;
  int found;

  /* Process whole words first, seeking for first word that is not
   * all ones or all zeros respectively if we are lookig for zeros
   * or ones. This is much faster with large strings having contiguous
   * blocks of 1 or 0 bits compared to the vanilla bit per bit processing.
   *
   * Note that if we start from an address that is not aligned
   * to sizeof(unsigned long) we consume it byte by byte until it is
   * aligned. */

  /* Skip initial bits not aligned to sizeof(unsigned long) byte by byte. */
  skipval = bit ? 0 : UCHAR_MAX;
  c = (unsigned char *)s;
  found = 0;
  while ((unsigned long)c & (sizeof(*l) - 1) && count) {
    if (*c != skipval) {
      found = 1;
      break;
    }
    c++;
    count--;
    pos += 8;
  }

  /* Skip bits with full word step. */
  l = (unsigned long *)c;
  if (!found) {
    skipval = bit ? 0 : ULONG_MAX;
    while (count >= sizeof(*l)) {
      if (*l != skipval) break;
      l++;
      count -= sizeof(*l);
      pos += sizeof(*l) * 8;
    }
  }

  /* Load bytes into "word" considering the first byte as the most significant
   * (we basically consider it as written in big endian, since we consider the
   * string as a set of bits from left to right, with the first bit at position
   * zero.
   *
   * Note that the loading is designed to work even when the bytes left
   * (count) are less than a full word. We pad it with zero on the right. */
  c = (unsigned char *)l;
  for (j = 0; j < sizeof(*l); j++) {
    word <<= 8;
    if (count) {
      word |= *c;
      c++;
      count--;
    }
  }

  /* Special case:
   * If bits in the string are all zero and we are looking for one,
   * return -1 to signal that there is not a single "1" in the whole
   * string. This can't happen when we are looking for "0" as we assume
   * that the right of the string is zero padded. */
  if (bit == 1 && word == 0) return -1;

  /* Last word left, scan bit by bit. The first thing we need is to
   * have a single "1" set in the most significant position in an
   * unsigned long. We don't know the size of the long so we use a
   * simple trick. */
  one = ULONG_MAX; /* All bits set to 1.*/
  one >>= 1;       /* All bits set to 1 but the MSB. */
  one = ~one;      /* All bits set to 0 but the MSB. */

  while (one) {
    if (((one & word) != 0) == bit) return pos;
    pos++;
    one >>= 1;
  }

  /* If we reached this point, there is a bug in the algorithm, since
   * the case of no match is handled as a special case before. */
  // Panic("End of redisBitpos() reached.");
  return -1; /* Just to avoid warnings. */
}

bool IsHexDigit(char c) {
  return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') ||
         (c >= 'A' && c <= 'F');
}

char HexDigitToInt(char c) {
  switch (c) {
    case '0':
      return 0;
    case '1':
      return 1;
    case '2':
      return 2;
    case '3':
      return 3;
    case '4':
      return 4;
    case '5':
      return 5;
    case '6':
      return 6;
    case '7':
      return 7;
    case '8':
      return 8;
    case '9':
      return 9;
    case 'a':
    case 'A':
      return 10;
    case 'b':
    case 'B':
      return 11;
    case 'c':
    case 'C':
      return 12;
    case 'd':
    case 'D':
      return 13;
    case 'e':
    case 'E':
      return 14;
    case 'f':
    case 'F':
      return 15;
    default:
      return 0;
  }
  return 0;
}

uint32_t NextPower(uint32_t size) {
  if (size > INT32_MAX) return uint32_t(INT32_MAX) + 1;

  uint32_t i = 1;
  while (true) {
    if (i >= size) break;
    i <<= 1;
  }
  return i;
}

void RandomBytes(unsigned char bytes[], size_t len) {
  std::random_device rd;
  std::mt19937 mt(rd());
  for (size_t i = 0; i < len; i++) {
    bytes[i] = (unsigned char)(mt() / UINT8_MAX);
  }
}

int64_t GetDirectorySize(const std::string &dir) {
  DIR *dp;
  struct dirent *entry;
  struct stat statbuf;
  long long int totalSize = 0;

  if ((dp = opendir(dir.c_str())) == NULL) {
    // fprintf(stderr, "Cannot open dir: %s\n", dir);
    return -1;
  }

  lstat(dir.c_str(), &statbuf);
  totalSize += statbuf.st_size;

  while ((entry = readdir(dp)) != NULL) {
    char subdir[256];
    // sprintf(subdir, "%s/%s", dir, entry->d_name);
    lstat(subdir, &statbuf);

    if (S_ISDIR(statbuf.st_mode)) {
      if (strcmp(".", entry->d_name) == 0 || strcmp("..", entry->d_name) == 0) {
        continue;
      }

      long long int subDirSize = GetDirectorySize(subdir);
      totalSize += subDirSize;
    } else {
      totalSize += statbuf.st_size;
    }
  }

  closedir(dp);
  return totalSize;
}

std::string GetSizeString(int64_t size) {
  if (size < 0) {
    return "0";
  }

  int i = 0;
  std::vector<int> flags = {'B', 'K', 'M', 'G', 'T'};
  while (size > 1024 && i < flags.size()) {
    size >>= 10;
    i++;
  }

  return Format("%d%c", size, flags[i]);
}

void PrintHex(const char *data, size_t len) {
  for (size_t i = 0; i < len; ++i) {
    printf("0x%02x ", (unsigned char)data[i]);
  }
  printf("\n");
}
}  // namespace rockin
