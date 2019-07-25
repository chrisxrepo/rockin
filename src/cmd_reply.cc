#include "cmd_reply.h"
#include "utils.h"

namespace rockin {
BufPtrs ReplyNil() {
  static BufPtr g_nil = make_buffer("$-1\r\n");

  BufPtrs datas;
  datas.push_back(g_nil);
  return std::move(datas);
}

BufPtrs ReplyOk() {
  static BufPtr g_reply_ok = make_buffer("+OK\r\n");

  BufPtrs datas;
  datas.push_back(g_reply_ok);
  return std::move(datas);
}

BufPtrs ReplyIntegerError() {
  static BufPtr g_integer_err =
      make_buffer("-ERR value is not an integer or out of range\r\n");

  BufPtrs datas;
  datas.push_back(g_integer_err);
  return std::move(datas);
}

BufPtrs ReplySyntaxError() {
  static BufPtr g_syntax_err = make_buffer("-ERR syntax error\r\n");

  BufPtrs datas;
  datas.push_back(g_syntax_err);
  return std::move(datas);
}

BufPtrs ReplyError(BufPtr err) {
  static BufPtr g_begin_err = make_buffer("-");
  static BufPtr g_proto_split = make_buffer("\r\n");

  BufPtrs datas;
  datas.push_back(g_begin_err);
  datas.push_back(err);
  datas.push_back(g_proto_split);
  return std::move(datas);
}

BufPtrs ReplyTypeError() {
  static BufPtr g_reply_type_warn = make_buffer(
      "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");

  BufPtrs datas;
  datas.push_back(g_reply_type_warn);
  return std::move(datas);
}

BufPtrs ReplyString(BufPtr str) {
  if (str == nullptr) {
    return ReplyNil();
  }

  static BufPtr g_begin_str = make_buffer("+");
  static BufPtr g_proto_split = make_buffer("\r\n");

  BufPtrs datas;
  datas.push_back(g_begin_str);
  datas.push_back(str);
  datas.push_back(g_proto_split);
  return std::move(datas);
}

BufPtrs ReplyInteger(int64_t num) {
  static BufPtr g_begin_int = make_buffer(":");
  static BufPtr g_proto_split = make_buffer("\r\n");

  BufPtrs datas;
  datas.push_back(g_begin_int);
  datas.push_back(make_buffer(Int64ToString(num)));
  datas.push_back(g_proto_split);
  return std::move(datas);
}

BufPtrs ReplyBulk(BufPtr str) {
  if (str == nullptr) {
    return ReplyNil();
  }

  static BufPtr g_begin_bulk = make_buffer("$");
  static BufPtr g_proto_split = make_buffer("\r\n");

  BufPtrs datas;
  datas.push_back(g_begin_bulk);
  datas.push_back(make_buffer(Int64ToString(str->len)));
  datas.push_back(g_proto_split);
  datas.push_back(str);
  datas.push_back(g_proto_split);
  return std::move(datas);
}

BufPtrs ReplyArray(BufPtrs &values) {
  static BufPtr g_begin_array = make_buffer("*");
  static BufPtr g_begin_bulk = make_buffer("$");
  static BufPtr g_proto_split = make_buffer("\r\n");
  static BufPtr g_nil = make_buffer("$-1\r\n");

  BufPtrs datas;
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
  return std::move(datas);
}

BufPtrs ReplyObj(std::shared_ptr<object_t> obj) {
  if (obj == nullptr) {
    return ReplyNil();
  } else if (obj->type == Type_String && obj->encode == Encode_Raw) {
    auto str_value = std::static_pointer_cast<buffer_t>(obj->value);
    return ReplyBulk(str_value);
  } else if (obj->type == Type_String && obj->encode == Encode_Int) {
    auto str_value = std::static_pointer_cast<buffer_t>(obj->value);
    return ReplyInteger(*((int64_t *)str_value->data));
  }

  return BufPtrs();
}
}  // namespace rockin