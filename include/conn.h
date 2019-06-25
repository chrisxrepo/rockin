#pragma once
#include <uv.h>
#include <functional>
#include "byte_buf.h"
#include "utils.h"

namespace rockin {
class RedisCmd;
class Conn : public std::enable_shared_from_this<Conn> {
 public:
  Conn(uv_tcp_t *t, std::function<void(std::shared_ptr<Conn>)> close_cb);
  ~Conn();

  bool StartRead();
  bool StopRead();

  void Close();

  bool WriteData(std::vector<std::string> &&datas);

  uv_tcp_t *handle() { return t_; }

 private:
  void OnAlloc(size_t suggested_size, uv_buf_t *buf);
  void OnRead(ssize_t nread, const uv_buf_t *buf);

 private:
  uv_tcp_t *t_;
  ByteBuf buf_;
  std::shared_ptr<RedisCmd> cmd_;
};
}  // namespace rockin
