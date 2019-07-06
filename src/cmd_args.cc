#include "cmd_args.h"
#include <glog/logging.h>
#include <algorithm>
#include <sstream>
#include "rockin_conn.h"
#include "type_common.h"
#include "type_string.h"
#include "utils.h"

namespace rockin {
CmdArgs::CmdArgs() : mbulk_(-1) {}

std::shared_ptr<buffer_t> CmdArgs::Parse(ByteBuf &buf) {
  if (args_.size() == mbulk_) {
    return nullptr;
  }

  if (buf.readable() == 0) {
    return nullptr;
  }

  char *ptr = buf.readptr();
  if (*ptr == '*' || mbulk_ > 0) {
    return ParseMultiCommand(buf);
  } else {
    return ParseInlineCommand(buf);
  }

  return nullptr;
}

std::shared_ptr<buffer_t> CmdArgs::ParseMultiCommand(ByteBuf &buf) {
  char *ptr = buf.readptr();
  if (*ptr == '*') {
    char *end = Strchr2(ptr, buf.readable(), '\r', '\n');
    if (end == nullptr) {
      return nullptr;
    }

    mbulk_ = (int)StringToInt64(ptr + 1, end - ptr - 1);
    if (mbulk_ <= 0 || mbulk_ > 1024 * 64) {
      std::ostringstream build;
      build << "ERR Protocol error: invalid multi bulk length:"
            << std::string(ptr + 1, end - ptr - 1);
      return make_buffer(build.str());
    }

    buf.move_readptr(end - ptr + 2);
  }

  for (int i = args_.size(); i < mbulk_; i++) {
    char *nptr = buf.readptr();
    char *end = Strchr2(nptr, buf.readable(), '\r', '\n');
    if (end == nullptr) {
      return nullptr;
    }

    if (*nptr != '$') {
      std::ostringstream build;
      build << "ERR Protocol error: expected '$', got '" << *nptr << "'";
      return make_buffer(build.str());
    }

    int bulk = (int)StringToInt64(nptr + 1, end - nptr - 1);
    if (bulk < 0 || bulk > 1024 * 1024) {
      std::ostringstream build;
      build << "ERR Protocol error: invalid bulk length:"
            << std::string(nptr + 1, end - nptr - 1);
      return make_buffer(build.str());
    }

    char *dptr = end + 2;
    if (dptr - nptr + bulk + 2 > buf.readable()) {
      return nullptr;
    }

    if (*(dptr + bulk) != '\r' || *(dptr + bulk + 1) != '\n') {
      std::ostringstream build;
      build << "ERR Protocol error: invalid bulk length";
      return make_buffer(build.str());
    }

    args_.push_back(make_buffer(dptr, bulk));
    buf.move_readptr(dptr - nptr + bulk + 2);
  }

  return nullptr;
}

std::shared_ptr<buffer_t> CmdArgs::ParseInlineCommand(ByteBuf &buf) {
  char *ptr = buf.readptr();
  char *end = Strchr2(ptr, buf.readable(), '\r', '\n');
  if (end == nullptr) {
    return nullptr;
  }

  while (ptr < end) {
    while (*ptr == ' ') ptr++;
    if (ptr >= end) break;

    if (*ptr == '"') {
      ptr++;
      std::ostringstream build;
      while (ptr < end) {
        if (end - ptr >= 4 && *ptr == '\\' && *(ptr + 1) == 'x' &&
            IsHexDigit(*(ptr + 2)) && IsHexDigit(*(ptr + 3))) {
          build << char(HexDigitToInt(*(ptr + 2)) * 16 +
                        HexDigitToInt(*(ptr + 3)));
          ptr += 4;
        } else if (end - ptr >= 2 && *ptr == '\\') {
          switch (*(ptr + 1)) {
            case 'n':
              build << '\n';
              break;
            case 'r':
              build << '\r';
              break;
            case 't':
              build << '\t';
              break;
            case 'b':
              build << '\b';
              break;
            case 'a':
              build << '\a';
              break;
            default:
              build << *(ptr + 1);
          }
          ptr += 2;
        } else if (*ptr == '"') {
          ptr++;
          break;
        } else {
          build << *ptr;
          ptr++;
        }
      }
      args_.push_back(make_buffer(build.str()));

    } else if (*ptr == '\'') {
      ptr++;
      std::ostringstream build;
      while (ptr < end) {
        if (end - ptr >= 2 && *ptr == '\\' && *(ptr + 1) == '\'') {
          build << '\'';
          ptr += 2;
        } else if (*ptr == '\'') {
          ptr++;
          break;
        } else {
          build << *ptr;
          ptr++;
        }
      }
      args_.push_back(make_buffer(build.str()));

    } else {
      char *space = Strchr(ptr, end - ptr, ' ');
      if (space == nullptr) {
        space = end;
      }
      args_.push_back(make_buffer(ptr, space - ptr));
      ptr = space;
    }
  }

  mbulk_ = args_.size();
  buf.move_readptr(end - buf.readptr() + 2);
  return nullptr;
}

std::string CmdArgs::ToString() {
  std::ostringstream build;
  for (int i = 0; i < args_.size(); ++i) {
    build << std::string(args_[i]->data, args_[i]->len) << " ";
  }

  return build.str();
}

}  // namespace rockin