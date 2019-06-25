#ifndef _BYTEBUF_H_
#define _BYTEBUF_H_
#include <stdio.h>
#include <stdlib.h>

namespace rockin {
class ByteBuf {
 public:
  ByteBuf(size_t cap) : cap_(cap), read_(0), write_(0) {
    data_ = (char *)malloc(cap);
  }
  ~ByteBuf() { free(data_); }

  size_t readable() { return write_ - read_; }

  char *readptr() { return data_ + read_; }

  void move_readptr(size_t size) {
    read_ += size;
    if (read_ > write_) read_ = write_;
  }

  size_t writeable() { return cap_ - write_; }

  char *writeptr() { return data_ + write_; }

  void move_writeptr(size_t size) {
    write_ += size;
    if (write_ > cap_) write_ = cap_;
  }

  void expand() {
    if (cap_ >= 0x10000) {
      cap_ += 0x10000;
    } else {
      cap_ *= 2;
    }
    data_ = (char *)realloc(data_, cap_);
  }

 private:
  char *data_;
  size_t cap_;
  size_t read_, write_;
};

}  // namespace rockin
#endif