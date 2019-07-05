#include "rockin_server.h"
#include <glog/logging.h>
#include <iostream>
#include <thread>
#include "event_loop.h"
#include "rockin_conn.h"
#include "utils.h"
#ifdef _WIN32
#include <windows.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace rockin {

namespace {
std::once_flag rockin_server_once_flag;
RockinServer *g_rockin_server;
};  // namespace

RockinServer *RockinServer::Default() {
  std::call_once(rockin_server_once_flag,
                 []() { g_rockin_server = new RockinServer(); });
  return g_rockin_server;
}

RockinServer::RockinServer() : sock_(-1) {
  //....
  uv_mutex_init(&mutex_);
}

RockinServer::~RockinServer() {}

void RockinServer::Init(size_t thread_num) {
  for (int i = 0; i < thread_num; i++) {
    EventLoop *et = new EventLoop();
    et->Start();

    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    loops_.push_back(et);
  }
}

bool RockinServer::Service(int port) {
  sock_ = socket(AF_INET, SOCK_STREAM, 0);
  if (sock_ < 0) {
    LOG(ERROR) << "socket error:" << GetCerr();
    return false;
  }

  if (SetCloseOnExec(sock_) == false) {
    LOG(ERROR) << "SetCloseOnExec error:" << GetCerr();
    return false;
  }

  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = INADDR_ANY;
  addr.sin_port = htons(port);

  int ret = ::bind(sock_, (struct sockaddr *)&addr, sizeof(addr));
  if (ret < 0) {
    LOG(ERROR) << "bind error:" << GetCerr();
#ifndef _WIN32
    return ::close(sock_);
#else
    return closesocket(sock_);
#endif
    return false;
  }

  if (ListenSocket() == false) {
    LOG(ERROR) << "Listern RockinServer :" << port << " error.";
    return false;
  }

  LOG(INFO) << "Listern RockinServer :" << port << " success.";
  return true;
}
void RockinServer::Close() {
#ifndef _WIN32
  ::close(sock_);
#else
  closesocket(sock_);
#endif
}

bool RockinServer::ListenSocket() {
  int ret = 0, one = 1;

  if (SetReuseAddr(sock_) == false) {
    LOG(ERROR) << "SetReuseAddr error:" << GetCerr();
    return false;
  }

  if (SetReusePort(sock_) == false) {
    LOG(ERROR) << "SetReusePort error:" << GetCerr();
    return false;
  }

  for (int i = 0; i < loops_.size(); i++) {
    EventLoop *et = loops_[i];

    bool result = true;
    int sock = sock_;
    et->RunInLoopAndWait(
        [sock, &result](EventLoop *et, std::shared_ptr<void> arg) {
          uv_tcp_t *t = (uv_tcp_t *)malloc(sizeof(uv_tcp_t));
          int ret = uv_tcp_init(et->loop(), t);
          if (ret != 0) {
            result = false;
            return;
          }

          t->data = et;
          ret = uv_tcp_open(t, sock);
          if (ret != 0) {
            result = false;
            return;
          }

          ret = uv_listen((uv_stream_t *)t, 10000,
                          [](uv_stream_t *server, int status) {
                            RockinServer::Default()->OnAccept(
                                (EventLoop *)server->data, server, status);
                          });

          if (ret != 0) {
            result = false;
            return;
          }
        },
        nullptr);

    if (result == false) {
      return false;
    }
  }

  return true;
}

void RockinServer::OnAccept(EventLoop *et1, uv_stream_t *server, int status) {
  uv_tcp_t *t = (uv_tcp_t *)malloc(sizeof(uv_tcp_t));
  int retcode = uv_tcp_init(server->loop, t);
  if (retcode != 0) {
    LOG(ERROR) << "uv_tcp_init error:" << GetUvError(retcode);
    return;
  }

  retcode = uv_accept(server, (uv_stream_t *)t);
  if (retcode != 0) {
    LOG(ERROR) << "uv_accept error:" << GetUvError(retcode);
    return;
  }

  retcode = uv_stream_set_blocking((uv_stream_t *)t, 0);
  if (retcode != 0) {
    LOG(ERROR) << "uv_stream_set_blocking error:" << GetUvError(retcode);
    return;
  }

  retcode = uv_tcp_nodelay(t, 1);
  if (retcode != 0) {
    LOG(ERROR) << "uv_tcp_nodelay error:" << GetUvError(retcode);
    return;
  }

  auto conn =
      std::make_shared<RockinConn>(t, [this](std::shared_ptr<RockinConn> conn) {
        // LOG(INFO) << "remove from list";
        uv_mutex_lock(&mutex_);
        this->conns_.erase(conn);
        uv_mutex_unlock(&mutex_);
        free(conn->handle());
      });

  conn->StartRead();

  uv_mutex_lock(&mutex_);
  conns_.insert(std::move(conn));
  uv_mutex_unlock(&mutex_);
}

}  // namespace rockin