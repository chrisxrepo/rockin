#include "cmd_args.h"
#include <glog/logging.h>
#include <algorithm>
#include <sstream>
#include "rockin_conn.h"
#include "type_common.h"
#include "type_string.h"
#include "utils.h"

namespace rockin {
std::string _emptyStr = "";

typedef std::function<void(std::shared_ptr<CmdArgs>)> HandleFunc;
struct redisHandle {
  std::string cmd;
  HandleFunc func;
  int arity;

  redisHandle(const std::string &cmd, const HandleFunc &func, int arity)
      : cmd(cmd), func(func), arity(arity) {}
  redisHandle() {}
};

static std::unordered_map<std::string, redisHandle> g_handle_map;

#define ADD_HANDLE(cmd, handle, arity) \
  g_handle_map[cmd] = redisHandle(cmd, FUN_STATIC_BIND1(handle), arity)

void CmdArgs::InitHandle() {
  /* ADD_HANDLE("ping", PingCommand, -1);
  ADD_HANDLE("info", InfoCommand, -1);
  ADD_HANDLE("command", CommandCommand, -1);
  ADD_HANDLE("select", SelectCommand, 2);
  ADD_HANDLE("flushdb", FlushDBCommand, 1);
  ADD_HANDLE("flushall", FlushAllCommand, 1);
  ADD_HANDLE("del", DelCommand, -2);
  ADD_HANDLE("get", GetCommand, 2);
  ADD_HANDLE("set", SetCommand, -3);
  ADD_HANDLE("append", AppendCommand, 3);
  ADD_HANDLE("getset", GetSetCommand, 3);
  ADD_HANDLE("mget", MGetCommand, -2);
  ADD_HANDLE("mset", MSetCommand, -3);
  ADD_HANDLE("incr", IncrCommand, 2);
  ADD_HANDLE("incrby", IncrbyCommand, 3);
  ADD_HANDLE("decr", DecrCommand, 2);
  ADD_HANDLE("decrby", DecrbyCommand, 3);
  ADD_HANDLE("setbit", SetBitCommand, 4);
  ADD_HANDLE("getbit", GetBitCommand, 3);
  ADD_HANDLE("bitcount", BitCountCommand, -2);
  ADD_HANDLE("bitop", BitOpCommand, -4);
  ADD_HANDLE("bitpos", BitPosCommand, -3);
  */
}

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

bool CmdArgs::OK() {
  //....
  return args_.size() == mbulk_;
}

void CmdArgs::Handle() {
  if (args_.size() != mbulk_) {
    return;
  }

  std::string cmd(args_[0]->data, args_[0]->len);
  std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::tolower);
  auto iter = g_handle_map.find(cmd);
  if (iter == g_handle_map.end()) {
    std::ostringstream build;
    build << "ERR unknown command `" << cmd << "`, with args beginning with: ";
    for (int i = 1; i < args_.size(); i++) build << "`" << args_[i] << "`, ";
    ReplyError(make_buffer(build.str()));
    return;
  }

  if ((iter->second.arity > 0 && iter->second.arity != args_.size()) ||
      int(args_.size()) < -iter->second.arity) {
    std::ostringstream build;
    build << "ERR wrong number of arguments for '" << cmd << "' command";
    ReplyError(make_buffer(build.str()));
    return;
  }

  iter->second.func(shared_from_this());
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

void CmdArgs::CloseConn() {
  auto conn = conn_.lock();
  if (conn != nullptr) {
    conn->Close();
  }
}

/////////////////////////////////////////////////////////////////
std::shared_ptr<buffer_t> CmdArgs::g_nil = make_buffer("$-1\r\n");
std::shared_ptr<buffer_t> CmdArgs::g_begin_err = make_buffer("-");
std::shared_ptr<buffer_t> CmdArgs::g_begin_str = make_buffer("+");
std::shared_ptr<buffer_t> CmdArgs::g_begin_int = make_buffer(":");
std::shared_ptr<buffer_t> CmdArgs::g_begin_array = make_buffer("*");
std::shared_ptr<buffer_t> CmdArgs::g_begin_bulk = make_buffer("$");
std::shared_ptr<buffer_t> CmdArgs::g_proto_split = make_buffer("\r\n");
std::shared_ptr<buffer_t> CmdArgs::g_reply_ok = make_buffer("+OK\r\n");
std::shared_ptr<buffer_t> CmdArgs::g_reply_type_warn = make_buffer(
    "WRONGTYPE Operation against a key holding the wrong kind of value");
std::shared_ptr<buffer_t> CmdArgs::g_reply_dbindex_invalid =
    make_buffer("ERR invalid DB index");
std::shared_ptr<buffer_t> CmdArgs::g_reply_dbindex_range =
    make_buffer("ERR DB index is out of range");
std::shared_ptr<buffer_t> CmdArgs::g_reply_syntax_err =
    make_buffer("ERR syntax error");
std::shared_ptr<buffer_t> CmdArgs::g_reply_mset_args_err =
    make_buffer("ERR wrong number of arguments for MSET");
std::shared_ptr<buffer_t> CmdArgs::g_reply_integer_err =
    make_buffer("ERR value is not an integer or out of range");
std::shared_ptr<buffer_t> CmdArgs::g_reply_nan_err =
    make_buffer("ERR would produce NaN or Infinity");
std::shared_ptr<buffer_t> CmdArgs::g_reply_bit_err =
    make_buffer("bit offset is not an integer or out of range");

void CmdArgs::ReplyNil() {
  auto conn = conn_.lock();
  if (conn == nullptr) {
    return;
  }

  std::vector<std::shared_ptr<buffer_t>> datas;
  datas.push_back(g_nil);
  conn->WriteData(std::move(datas));
}

void CmdArgs::ReplyOk() {
  auto conn = conn_.lock();
  if (conn == nullptr) {
    return;
  }

  std::vector<std::shared_ptr<buffer_t>> datas;
  datas.push_back(g_reply_ok);
  conn->WriteData(std::move(datas));
}

void CmdArgs::ReplyError(std::shared_ptr<buffer_t> err) {
  auto conn = conn_.lock();
  if (conn == nullptr) {
    return;
  }

  std::vector<std::shared_ptr<buffer_t>> datas;
  datas.push_back(g_begin_err);
  datas.push_back(err);
  datas.push_back(g_proto_split);
  conn->WriteData(std::move(datas));
}

void CmdArgs::ReplyString(std::shared_ptr<buffer_t> str) {
  if (str == nullptr) {
    ReplyNil();
    return;
  }

  auto conn = conn_.lock();
  if (conn == nullptr) {
    return;
  }

  std::vector<std::shared_ptr<buffer_t>> datas;
  datas.push_back(g_begin_str);
  datas.push_back(str);
  datas.push_back(g_proto_split);
  conn->WriteData(std::move(datas));
}

void CmdArgs::ReplyInteger(int64_t num) {
  auto conn = conn_.lock();
  if (conn == nullptr) {
    return;
  }

  std::vector<std::shared_ptr<buffer_t>> datas;
  datas.push_back(g_begin_int);
  datas.push_back(make_buffer(Int64ToString(num)));
  datas.push_back(g_proto_split);
  conn->WriteData(std::move(datas));
}

void CmdArgs::ReplyBulk(std::shared_ptr<buffer_t> str) {
  if (str == nullptr) {
    ReplyNil();
    return;
  }

  auto conn = conn_.lock();
  if (conn == nullptr) {
    return;
  }

  std::vector<std::shared_ptr<buffer_t>> datas;
  datas.push_back(g_begin_bulk);
  datas.push_back(make_buffer(Int64ToString(str->len)));
  datas.push_back(g_proto_split);
  datas.push_back(str);
  datas.push_back(g_proto_split);
  conn->WriteData(std::move(datas));
}

void CmdArgs::ReplyArray(std::vector<std::shared_ptr<buffer_t>> &values) {
  auto conn = conn_.lock();
  if (conn == nullptr) {
    return;
  }

  std::vector<std::shared_ptr<buffer_t>> datas;
  datas.push_back(g_begin_array);
  datas.push_back(make_buffer(Int64ToString(values.size())));
  datas.push_back(g_proto_split);
  for (size_t i = 0; i < values.size(); i++) {
    if (values[i] == nullptr) {
      datas.push_back(g_nil);
    } else {
      datas.push_back(g_begin_bulk);
      datas.push_back(make_buffer(Int64ToString(values[i]->len)));
      datas.push_back(g_proto_split);
      datas.push_back(values[i]);
      datas.push_back(g_proto_split);
    }
  }
  conn->WriteData(std::move(datas));
}

int CmdArgs::DbIndex() {
  auto conn = conn_.lock();
  return conn == nullptr ? 0 : conn->index();
}

std::string CmdArgs::ToString() {
  std::ostringstream build;
  for (int i = 0; i < args_.size(); ++i) {
    build << std::string(args_[i]->data, args_[i]->len) << " ";
  }

  return build.str();
}

}  // namespace rockin