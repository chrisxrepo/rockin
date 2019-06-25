#include "server.h"
#include <glog/logging.h>
#include <iostream>
#include "conn.h"
#include "net_pool.h"
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
Server::Server() : sock_(-1) { uv_mutex_init(&mutex_); }

Server::~Server() {}

bool Server::Service(int port) {
  sock_ = socket(AF_INET, SOCK_STREAM, 0);
  if (sock_ < 0) {
    LOG(ERROR) << "socket error:" << GetCerr();
    return false;
  }

  /* if (SetNonBlocking(sock_) == false) {
    LOG(ERROR) << "SetNonBlocking error:" << GetCerr();
    return false;
  }*/

  if (SetCloseOnExec(sock_) == false) {
    LOG(ERROR) << "SetCloseOnExec error:" << GetCerr();
    return false;
  }

  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = INADDR_ANY;
  addr.sin_port = htons(port);

  int ret = ::bind(sock_, (struct sockaddr*)&addr, sizeof(addr));
  if (ret < 0) {
    LOG(ERROR) << "bind error:" << GetCerr();
#ifndef _WIN32
    return ::close(sock_);
#else
    return closesocket(sock_);
#endif
    return false;
  }

  ret = NetPool::GetInstance()->ListenSocket(
      sock_, [this](EventLoop* et, uv_stream_t* server, int status) {
        this->OnAccept(et, server, status);
      });

  if (ret == false) {
    LOG(ERROR) << "Listern server :" << port << " error.";
    return false;
  }

  LOG(INFO) << "Listern server :" << port << " success.";
  return true;
}
void Server::Close() {
#ifndef _WIN32
  ::close(sock_);
#else
  closesocket(sock_);
#endif
}

void Server::OnAccept(EventLoop* et, uv_stream_t* server, int status) {
  uv_tcp_t* t = (uv_tcp_t*)malloc(sizeof(uv_tcp_t));
  int retcode = uv_tcp_init(server->loop, t);
  if (retcode != 0) {
    LOG(ERROR) << "uv_tcp_init error:" << GetUvError(retcode);
    return;
  }

  retcode = uv_accept(server, (uv_stream_t*)t);
  if (retcode != 0) {
    LOG(ERROR) << "uv_accept error:" << GetUvError(retcode);
    return;
  }

  retcode = uv_stream_set_blocking((uv_stream_t*)t, 0);
  if (retcode != 0) {
    LOG(ERROR) << "uv_stream_set_blocking error:" << GetUvError(retcode);
    return;
  }

  retcode = uv_tcp_nodelay(t, 1);
  if (retcode != 0) {
    LOG(ERROR) << "uv_tcp_nodelay error:" << GetUvError(retcode);
    return;
  }

  auto conn = std::make_shared<Conn>(t, [this](std::shared_ptr<Conn> conn) {
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