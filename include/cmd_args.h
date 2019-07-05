#pragma once
#include <iostream>
#include <unordered_map>
#include <vector>
#include "byte_buf.h"
#include "utils.h"

namespace rockin {
class RockinConn;
class CmdArgs : public std::enable_shared_from_this<CmdArgs> {
 public:
  static void InitHandle();

  CmdArgs();

  std::shared_ptr<buffer_t> Parse(ByteBuf &buf);
  bool OK();

  void Handle();

  void ReplyNil();
  void ReplyOk();
  void ReplyError(std::shared_ptr<buffer_t> err);
  void ReplyString(std::shared_ptr<buffer_t> str);
  void ReplyInteger(int64_t num);
  void ReplyBulk(std::shared_ptr<buffer_t> str);
  void ReplyArray(std::vector<std::shared_ptr<buffer_t>> &values);

  std::shared_ptr<RockinConn> conn() { return conn_.lock(); }
  std::vector<std::shared_ptr<buffer_t>> &args() { return args_; }

  int DbIndex();
  std::string ToString();

 private:
  std::shared_ptr<buffer_t> ParseMultiCommand(ByteBuf &buf);
  std::shared_ptr<buffer_t> ParseInlineCommand(ByteBuf &buf);

  // close client conn
  void CloseConn();

 private:
  std::weak_ptr<RockinConn> conn_;
  std::vector<std::shared_ptr<buffer_t>> args_;
  int mbulk_;

 public:
  static std::shared_ptr<buffer_t> g_nil;
  static std::shared_ptr<buffer_t> g_begin_err;
  static std::shared_ptr<buffer_t> g_begin_str;
  static std::shared_ptr<buffer_t> g_begin_int;
  static std::shared_ptr<buffer_t> g_begin_array;
  static std::shared_ptr<buffer_t> g_begin_bulk;
  static std::shared_ptr<buffer_t> g_proto_split;
  static std::shared_ptr<buffer_t> g_reply_ok;
  static std::shared_ptr<buffer_t> g_reply_type_warn;
  static std::shared_ptr<buffer_t> g_reply_dbindex_invalid;
  static std::shared_ptr<buffer_t> g_reply_dbindex_range;
  static std::shared_ptr<buffer_t> g_reply_syntax_err;
  static std::shared_ptr<buffer_t> g_reply_mset_args_err;
  static std::shared_ptr<buffer_t> g_reply_integer_err;
  static std::shared_ptr<buffer_t> g_reply_nan_err;
  static std::shared_ptr<buffer_t> g_reply_bit_err;
};

}  // namespace rockin