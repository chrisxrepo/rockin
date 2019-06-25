#pragma once
#include <set>
#include "utils.h"
#include "uv.h"

namespace rockin {
class Conn;
class EventLoop;
class Server {
 public:
  Server();
  ~Server();

  bool Service(int port);
  void Close();

 private:
  void OnAccept(EventLoop *et, uv_stream_t *server, int status);

 private:
  int sock_;
  std::set<std::shared_ptr<Conn>> conns_;
  uv_mutex_t mutex_;
};
}  // namespace rockin
