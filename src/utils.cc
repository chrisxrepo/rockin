#include "utils.h"
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <random>
#include <sstream>

#define MAX_LONG_DOUBLE_CHARS 5 * 1024

namespace rockin {

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

int StringToDouble(const char *s, size_t slen, double *dp) {
  char buf[MAX_LONG_DOUBLE_CHARS];
  double value;
  char *eptr;

  if (slen >= sizeof(buf)) return 0;
  memcpy(buf, s, slen);
  buf[slen] = '\0';

  errno = 0;
  value = strtold(buf, &eptr);
  if (isspace(buf[0]) || eptr[0] != '\0' ||
      (errno == ERANGE &&
       (value == HUGE_VAL || value == -HUGE_VAL || value == 0)) ||
      errno == EINVAL || isnan(value))
    return 0;

  if (dp) *dp = value;
  return 1;
}

double StringToDouble(const char *s, size_t slen) {
  double value;
  if (StringToDouble(s, slen, &value) == 0) return 0.0f;
  return value;
}

int DoubleToString(char *buf, size_t len, double value, int human) {
  size_t l;

  if (isinf(value)) {
    /* Libc in odd systems (Hi Solaris!) will format infinite in a
     * different way, so better to handle it in an explicit way. */
    if (len < 5) return 0; /* No room. 5 is "-inf\0" */
    if (value > 0) {
      memcpy(buf, "inf", 3);
      l = 3;
    } else {
      memcpy(buf, "-inf", 4);
      l = 4;
    }
  } else if (human) {
    /* We use 17 digits precision since with 128 bit floats that precision
     * after rounding is able to represent most small decimal numbers in a
     * way that is "non surprising" for the user (that is, most small
     * decimal numbers will be represented in a way that when converted
     * back into a string are exactly the same as what the user typed.) */
    l = snprintf(buf, len, "%.17Lf", (long double)value);
    if (l + 1 > len) return 0; /* No room. */
    /* Now remove trailing zeroes after the '.' */
    if (strchr(buf, '.') != NULL) {
      char *p = buf + l - 1;
      while (*p == '0') {
        p--;
        l--;
      }
      if (*p == '.') l--;
    }
  } else {
    l = snprintf(buf, len, "%.17Lg", (long double)value);
    if (l + 1 > len) return 0; /* No room. */
  }
  buf[l] = '\0';
  return l;
}

std::string DoubleToString(double v, int human) {
  char buf[MAX_LONG_DOUBLE_CHARS];
  int l = DoubleToString(buf, MAX_LONG_DOUBLE_CHARS, v, human);
  return std::string(buf, l);
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

void PrintHex(const char *data, size_t len) {
  for (size_t i = 0; i < len; ++i) {
    printf("0x%02x ", (unsigned char)data[i]);
  }
  printf("\n");
}
}  // namespace rockin
