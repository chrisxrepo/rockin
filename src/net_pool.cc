#include "net_pool.h"
#include <glog/logging.h>
#include <thread>
#include "utils.h"

namespace rockin {

NetPool *NetPool::g_net_pool_ = nullptr;

NetPool *NetPool::GetInstance() {
  if (g_net_pool_ == nullptr) {
    g_net_pool_ = new NetPool();
  }

  return g_net_pool_;
}

void NetPool::Init(int thread_num) {
  for (int i = 0; i < thread_num; i++) {
    EventLoop *et = new EventLoop();
    et->Start();

    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    loops_.push_back(et);
  }
}

class ServerData {
 public:
  EventLoop *loop;
  std::function<void(EventLoop *, uv_stream_t *, int)> callback;
};

bool NetPool::ListenSocket(
    int sock, std::function<void(EventLoop *, uv_stream_t *, int)> cb) {
  int ret = 0, one = 1;

  if (SetReuseAddr(sock) == false) {
    LOG(ERROR) << "SetReuseAddr error:" << GetCerr();
    return false;
  }

  if (SetReusePort(sock) == false) {
    LOG(ERROR) << "SetReusePort error:" << GetCerr();
    return false;
  }

  for (int i = 0; i < loops_.size(); i++) {
    EventLoop *et = loops_[i];

    bool result = true;
    et->RunInLoopAndWait([sock, cb, &result](EventLoop *et) {
      uv_tcp_t *t = (uv_tcp_t *)malloc(sizeof(uv_tcp_t));
      int ret = uv_tcp_init(et->loop(), t);
      if (ret != 0) {
        result = false;
        return;
      }

      ServerData *sd = new ServerData;
      sd->loop = et;
      sd->callback = cb;
      t->data = sd;
      ret = uv_tcp_open(t, sock);
      if (ret != 0) {
        result = false;
        return;
      }

      ret = uv_listen((uv_stream_t *)t, 10000,
                      [](uv_stream_t *server, int status) {
                        ServerData *sd = (ServerData *)server->data;
                        sd->callback(sd->loop, server, status);
                      });

      if (ret != 0) {
        result = false;
        return;
      }
    });

    if (result == false) {
      return false;
    }
  }

  return true;
}

void NetPool::Stop() {
  for (size_t i = 0; i < loops_.size(); i++) {
    loops_[i]->Stop();
  }
}

}  // namespace rockin