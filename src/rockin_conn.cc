#include "rockin_conn.h"
#include <glog/logging.h>
#include "cmd_args.h"
#include "cmd_table.h"
#include "event_loop.h"
#include "mem_db.h"

namespace rockin {
class _ConnData {
 public:
  std::weak_ptr<RockinConn> weak_ptr;
  std::function<void(std::shared_ptr<RockinConn>)> close_cb;
};

RockinConn::RockinConn(
    uv_tcp_t *t, std::function<void(std::shared_ptr<RockinConn>)> close_cb)
    : index_(0), t_(t), buf_(4096) {
  _ConnData *cd = new _ConnData;
  cd->close_cb = close_cb;
  t->data = cd;
}

RockinConn::~RockinConn() {
  // LOG(INFO) << "conn destory";
}

bool RockinConn::StartRead() {
  if (t_ == nullptr) {
    return false;
  }

  _ConnData *cd = (_ConnData *)t_->data;
  cd->weak_ptr = shared_from_this();

  bool result = true;
  EventLoop *el = (EventLoop *)t_->loop->data;
  std::weak_ptr<RockinConn> weak_conn = shared_from_this();
  el->RunInLoopAndWait(
      [&result, weak_conn](EventLoop *et, std::shared_ptr<void> arg) {
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
      },
      nullptr);

  return result;
}

bool RockinConn::StopRead() {
  if (t_ == nullptr) {
    return false;
  }

  bool result = true;
  EventLoop *el = (EventLoop *)t_->loop->data;
  std::weak_ptr<RockinConn> weak_conn = shared_from_this();
  el->RunInLoopAndWait(
      [&result, weak_conn](EventLoop *et, std::shared_ptr<void> arg) {
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
      },
      nullptr);

  return result;
}

void RockinConn::Close() {
  EventLoop *el = (EventLoop *)t_->loop->data;
  std::weak_ptr<RockinConn> weak_conn = shared_from_this();
  el->RunInLoopNoWait(
      [weak_conn](EventLoop *et, std::shared_ptr<void> arg) {
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
      },
      nullptr);
}

class _WriteData {
 public:
  const std::vector<MemPtr> datas;
  uv_buf_t *bufs;

  _WriteData(const std::vector<MemPtr> &&d) : datas(d) {
    bufs = (uv_buf_t *)malloc(sizeof(uv_buf_t) * datas.size());
    for (int i = 0; i < datas.size(); ++i)
      *(bufs + i) = uv_buf_init(datas[i]->data, datas[i]->len);
  }

  ~_WriteData() { free(bufs); }
};

bool RockinConn::WriteData(std::vector<MemPtr> &&datas) {
  if (t_ == nullptr) {
    return false;
  }

  EventLoop *el = (EventLoop *)t_->loop->data;
  std::weak_ptr<RockinConn> weak_conn = shared_from_this();
  el->RunInLoopNoWait(
      [weak_conn, datas = std::move(datas)](EventLoop *et,
                                            std::shared_ptr<void> arg) {
        auto conn = weak_conn.lock();
        if (conn == nullptr || conn->handle() == nullptr) {
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
      },
      nullptr);

  return true;
}

void RockinConn::OnAlloc(size_t suggested_size, uv_buf_t *buf) {
  if (buf_.writeable() == 0) {
    buf_.expand();
  }

  *buf = uv_buf_init(buf_.writeptr(), buf_.writeable());
}

void RockinConn::OnRead(ssize_t nread, const uv_buf_t *buf) {
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

  while (true) {
    if (cmd_args_ == nullptr) {
      cmd_args_ = std::make_shared<CmdArgs>();
    }

    auto errstr = cmd_args_->Parse(buf_);
    if (errstr != nullptr) {
      return ReplyErrorAndClose(errstr);
    }

    if (cmd_args_->is_ok() == false) {
      break;
    }

    if (cmd_args_->args().size() == 0) {
      cmd_args_.reset();
      continue;
    }

    auto &args = cmd_args_->args();
    if (args[0]->len == 4 && args[0]->data[0] == 'q' &&
        args[0]->data[1] == 'u' && args[0]->data[2] == 'i' &&
        args[0]->data[3] == 't') {
      ReplyOk();
      return Close();
    }

    CmdTable::Default()->HandeCmd(shared_from_this(), cmd_args_);
    cmd_args_.reset();
  }
}

//////////////////////////////////////////////////////

void RockinConn::ReplyNil() {
  static MemPtr g_nil = rockin::make_shared<membuf_t>("$-1\r\n");
  std::vector<MemPtr> datas;
  datas.push_back(g_nil);
  WriteData(std::move(datas));
}

void RockinConn::ReplyOk() {
  static MemPtr g_reply_ok = rockin::make_shared<membuf_t>("+OK\r\n");
  std::vector<MemPtr> datas;
  datas.push_back(g_reply_ok);
  WriteData(std::move(datas));
}

void RockinConn::ReplyIntegerError() {
  static MemPtr g_integer_err = rockin::make_shared<membuf_t>(
      "-ERR value is not an integer or out of range\r\n");
  std::vector<MemPtr> datas;
  datas.push_back(g_integer_err);
  WriteData(std::move(datas));
}

void RockinConn::ReplySyntaxError() {
  static MemPtr g_syntax_err =
      rockin::make_shared<membuf_t>("-ERR syntax error\r\n");
  std::vector<MemPtr> datas;
  datas.push_back(g_syntax_err);
  WriteData(std::move(datas));
}

void RockinConn::ReplyError(MemPtr err) {
  static MemPtr g_begin_err = rockin::make_shared<membuf_t>("-");
  static MemPtr g_proto_split = rockin::make_shared<membuf_t>("\r\n");

  std::vector<MemPtr> datas;
  datas.push_back(g_begin_err);
  datas.push_back(err);
  datas.push_back(g_proto_split);
  WriteData(std::move(datas));
}

void RockinConn::ReplyTypeError() {
  static MemPtr g_reply_type_warn = rockin::make_shared<membuf_t>(
      "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");

  std::vector<MemPtr> datas;
  datas.push_back(g_reply_type_warn);
  WriteData(std::move(datas));
}

void RockinConn::ReplyErrorAndClose(MemPtr err) {
  ReplyError(err);
  Close();
}

void RockinConn::ReplyString(MemPtr str) {
  if (str == nullptr) {
    ReplyNil();
    return;
  }

  static MemPtr g_begin_str = rockin::make_shared<membuf_t>("+");
  static MemPtr g_proto_split = rockin::make_shared<membuf_t>("\r\n");

  std::vector<MemPtr> datas;
  datas.push_back(g_begin_str);
  datas.push_back(str);
  datas.push_back(g_proto_split);
  WriteData(std::move(datas));
}

void RockinConn::ReplyInteger(int64_t num) {
  static MemPtr g_begin_int = rockin::make_shared<membuf_t>(":");
  static MemPtr g_proto_split = rockin::make_shared<membuf_t>("\r\n");

  std::vector<MemPtr> datas;
  datas.push_back(g_begin_int);
  datas.push_back(rockin::make_shared<membuf_t>(Int64ToString(num)));
  datas.push_back(g_proto_split);
  WriteData(std::move(datas));
}

void RockinConn::ReplyBulk(MemPtr str) {
  if (str == nullptr) {
    ReplyNil();
    return;
  }

  static MemPtr g_begin_bulk = rockin::make_shared<membuf_t>("$");
  static MemPtr g_proto_split = rockin::make_shared<membuf_t>("\r\n");

  std::vector<MemPtr> datas;
  datas.push_back(g_begin_bulk);
  datas.push_back(rockin::make_shared<membuf_t>(Int64ToString(str->len)));
  datas.push_back(g_proto_split);
  datas.push_back(str);
  datas.push_back(g_proto_split);
  WriteData(std::move(datas));
}

void RockinConn::ReplyArray(std::vector<MemPtr> &values) {
  static MemPtr g_begin_array = rockin::make_shared<membuf_t>("*");
  static MemPtr g_begin_bulk = rockin::make_shared<membuf_t>("$");
  static MemPtr g_proto_split = rockin::make_shared<membuf_t>("\r\n");
  static MemPtr g_nil = rockin::make_shared<membuf_t>("$-1\r\n");

  std::vector<MemPtr> datas;
  datas.push_back(g_begin_array);
  datas.push_back(rockin::make_shared<membuf_t>(Int64ToString(values.size())));
  datas.push_back(g_proto_split);
  for (size_t i = 0; i < values.size(); i++) {
    if (values[i] == nullptr) {
      datas.push_back(g_nil);
    } else {
      datas.push_back(g_begin_bulk);
      datas.push_back(
          rockin::make_shared<membuf_t>(Int64ToString(values[i]->len)));
      datas.push_back(g_proto_split);
      datas.push_back(values[i]);
      datas.push_back(g_proto_split);
    }
  }
  WriteData(std::move(datas));
}

void RockinConn::ReplyObj(std::shared_ptr<MemObj> obj) {
  if (obj == nullptr) {
    ReplyNil();
  } else if (obj->type == Type_String && obj->encode == Encode_Raw) {
    auto str_value = std::static_pointer_cast<membuf_t>(obj->value);
    ReplyBulk(str_value);
  } else if (obj->type == Type_String && obj->encode == Encode_Int) {
    auto str_value = std::static_pointer_cast<membuf_t>(obj->value);
    ReplyInteger(BUF_INT64(str_value));
  }
}

}  // namespace rockin