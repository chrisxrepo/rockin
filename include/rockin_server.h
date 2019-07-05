#pragma once
#include <set>
#include "utils.h"
#include "uv.h"

namespace rockin {
class RockinConn;
class EventLoop;
class RockinServer {
 public:
  RockinServer();
  ~RockinServer();

  static RockinServer *Default();

  void Init(size_t thread_num);
  bool Service(int port);
  void Close();

 private:
  bool ListenSocket();
  void OnAccept(EventLoop *et, uv_stream_t *RockinServer, int status);

 private:
  int sock_;
  std::set<std::shared_ptr<RockinConn>> conns_;
  std::vector<EventLoop *> loops_;
  uv_mutex_t mutex_;
};
}  // namespace rockin
