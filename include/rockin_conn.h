#pragma once
#include <uv.h>
#include <functional>
#include "byte_buf.h"
#include "rockin_alloc.h"
#include "utils.h"

namespace rockin {
class CmdArgs;
class RockinConn : public std::enable_shared_from_this<RockinConn> {
 public:
  RockinConn(uv_tcp_t *t,
             std::function<void(std::shared_ptr<RockinConn>)> close_cb);
  ~RockinConn();

  bool StartRead();
  bool StopRead();

  void Close();

  bool WriteData(std::vector<MemPtr> &&datas);

  uv_tcp_t *handle() { return t_; }

  int index() { return index_; }
  void set_index(int index) { index_ = index; }

  void ReplyNil();
  void ReplyOk();
  void ReplyIntegerError();
  void ReplySyntaxError();
  void ReplyError(MemPtr err);
  void ReplyErrorAndClose(MemPtr err);
  void ReplyString(MemPtr str);
  void ReplyInteger(int64_t num);
  void ReplyBulk(MemPtr str);
  void ReplyArray(std::vector<MemPtr> &values);

 private:
  void OnAlloc(size_t suggested_size, uv_buf_t *buf);
  void OnRead(ssize_t nread, const uv_buf_t *buf);

 private:
  int index_;
  uv_tcp_t *t_;
  ByteBuf buf_;
  std::shared_ptr<CmdArgs> cmd_args_;
};
}  // namespace rockin
