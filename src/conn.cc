#include "conn.h"
#include <glog/logging.h>
#include "event_loop.h"
#include "redis_cmd.h"

namespace rockin {
class _ConnData {
 public:
  std::weak_ptr<Conn> weak_ptr;
  std::function<void(std::shared_ptr<Conn>)> close_cb;
};

Conn::Conn(uv_tcp_t *t, std::function<void(std::shared_ptr<Conn>)> close_cb)
    : t_(t), buf_(4096) {
  _ConnData *cd = new _ConnData;
  cd->close_cb = close_cb;
  t->data = cd;
}

Conn::~Conn() {
  // LOG(INFO) << "conn destory";
}

bool Conn::StartRead() {
  if (t_ == nullptr) {
    return false;
  }

  _ConnData *cd = (_ConnData *)t_->data;
  cd->weak_ptr = shared_from_this();

  bool result = true;
  EventLoop *el = (EventLoop *)t_->loop->data;
  std::weak_ptr<Conn> weak_conn = shared_from_this();
  el->RunInLoopAndWait([&result, weak_conn](EventLoop *el) {
    auto conn = weak_conn.lock();
    if (conn == nullptr) {
      result = false;
      LOG(WARNING) << "Conn ptr is nullptr";
      return;
    }

    int ret = uv_read_start(
        (uv_stream_t *)conn->t_,
        [](uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf) {
          _ConnData *cd = (_ConnData *)handle->data;
          auto conn = cd->weak_ptr.lock();
          if (conn == nullptr) {
            LOG(WARNING) << "Conn ptr is nullptr";
            return;
          }
          conn->OnAlloc(suggested_size, buf);
        },
        [](uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf) {
          _ConnData *cd = (_ConnData *)stream->data;
          auto conn = cd->weak_ptr.lock();
          if (conn == nullptr) {
            LOG(WARNING) << "Conn ptr is nullptr";
            return;
          }
          conn->OnRead(nread, buf);
        });

    //   LOG(INFO) << "connect start to read.";
    if (ret != 0) {
      LOG(ERROR) << "uv_read_start error:" << GetUvError(ret);
      result = false;
    }
  });

  return result;
}

bool Conn::StopRead() {
  if (t_ == nullptr) {
    return false;
  }

  bool result = true;
  EventLoop *el = (EventLoop *)t_->loop->data;
  std::weak_ptr<Conn> weak_conn = shared_from_this();
  el->RunInLoopAndWait([&result, weak_conn](EventLoop *el) {
    auto conn = weak_conn.lock();
    if (conn == nullptr) {
      result = false;
      LOG(WARNING) << "Conn ptr is nullptr";
      return;
    }

    int ret = uv_read_stop((uv_stream_t *)conn->handle());
    if (ret != 0) {
      LOG(ERROR) << "uv_read_stop error:" << GetUvError(ret);
      result = false;
    }
  });

  return result;
}

void Conn::Close() {
  EventLoop *el = (EventLoop *)t_->loop->data;
  std::weak_ptr<Conn> weak_conn = shared_from_this();
  el->RunInLoopNoWait([weak_conn](EventLoop *el) {
    auto conn = weak_conn.lock();
    if (conn == nullptr) {
      return;
    }

    uv_close((uv_handle_t *)conn->handle(), [](uv_handle_t *handle) {
      // LOG(INFO) << "conncection close.";
      _ConnData *cd = (_ConnData *)handle->data;
      auto conn = cd->weak_ptr.lock();
      if (conn == nullptr) {
        return;
      }

      if (cd->close_cb) {
        cd->close_cb(conn);
      }

      conn->t_ = nullptr;
      delete cd;
    });
  });
}

class _WriteData {
 public:
  const std::vector<std::shared_ptr<buffer_t>> datas;
  uv_buf_t *bufs;

  _WriteData(const std::vector<std::shared_ptr<buffer_t>> &&d) : datas(d) {
    bufs = (uv_buf_t *)malloc(sizeof(uv_buf_t) * datas.size());
    for (int i = 0; i < datas.size(); ++i)
      *(bufs + i) = uv_buf_init(datas[i]->data, datas[i]->len);
  }

  ~_WriteData() { free(bufs); }
};

bool Conn::WriteData(std::vector<std::shared_ptr<buffer_t>> &&datas) {
  EventLoop *el = (EventLoop *)t_->loop->data;
  std::weak_ptr<Conn> weak_conn = shared_from_this();

  el->RunInLoopNoWait([weak_conn, datas](EventLoop *el) {
    auto conn = weak_conn.lock();
    if (conn == nullptr) {
      return;
    }

    _WriteData *write = new _WriteData(std::move(datas));
    uv_write_t *req = (uv_write_t *)malloc(sizeof(uv_write_t));
    req->data = write;

    uv_write(req, (uv_stream_t *)conn->handle(), write->bufs,
             write->datas.size(), [](uv_write_t *req, int status) {
               _WriteData *write = (_WriteData *)req->data;
               delete write;
               free(req);
             });
  });

  return true;
}

void Conn::OnAlloc(size_t suggested_size, uv_buf_t *buf) {
  if (buf_.writeable() == 0) {
    buf_.expand();
  }

  *buf = uv_buf_init(buf_.writeptr(), buf_.writeable());
}

void Conn::OnRead(ssize_t nread, const uv_buf_t *buf) {
  if (nread == 0) {
    return;
  }

  if (nread < 0) {
    if (nread != UV_EOF) {
      LOG(ERROR) << "Read error:" << GetUvError(nread);
    }
    this->Close();
    return;
  }

  buf_.move_writeptr(nread);
  if (cmd_ == nullptr) {
    cmd_ = std::make_shared<RedisCmd>(shared_from_this());
  }

  bool ret = cmd_->Parse(buf_);
  if (ret == false || cmd_->Args().size() == 0) {
    return;
  }

  auto &args = cmd_->Args();
  if (args[0]->len == 4 && args[0]->data[0] == 'q' && args[0]->data[1] == 'u' &&
      args[0]->data[2] == 'i' && args[0]->data[3] == 't') {
    cmd_->ReplyOk();
    return Close();
  }

  cmd_->Handle();
  cmd_.reset();
}

}  // namespace rockin