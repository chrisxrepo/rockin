#pragma once
#include <uv.h>
#include <functional>
#include "byte_buf.h"
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

  bool WriteData(std::vector<std::shared_ptr<buffer_t>> &&datas);

  uv_tcp_t *handle() { return t_; }

  int index() { return index_; }
  void set_index(int index) { index_ = index; }

  void ReplyNil();
  void ReplyOk();
  void ReplyError(std::shared_ptr<buffer_t> err);
  void ReplyErrorAndClose(std::shared_ptr<buffer_t> err);
  void ReplyString(std::shared_ptr<buffer_t> str);
  void ReplyInteger(int64_t num);
  void ReplyBulk(std::shared_ptr<buffer_t> str);
  void ReplyArray(std::vector<std::shared_ptr<buffer_t>> &values);

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
