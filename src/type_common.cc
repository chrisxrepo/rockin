#include "type_common.h"
#include <glog/logging.h>
#include <jemalloc/jemalloc.h>
#include "cmd_args.h"
#include "mem_db.h"
#include "mem_saver.h"
#include "rockin_conn.h"

namespace rockin {

void CommandCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                    std::shared_ptr<RockinConn> conn) {
  conn->ReplyOk();
}

void PingCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                 std::shared_ptr<RockinConn> conn) {
  static MemPtr g_reply_pong = rockin::make_shared<membuf_t>("PONG");
  conn->ReplyString(g_reply_pong);
}

void InfoCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                 std::shared_ptr<RockinConn> conn) {
  //....
  conn->ReplyOk();
}

void DelCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                std::shared_ptr<RockinConn> conn) {
  int cnt = cmd_args->args().size() - 1;
  auto rets = std::make_shared<MultiResult>(cnt);

  for (int i = 0; i < cnt; i++) {
    MemSaver::Default()->DoCmd(
        cmd_args->args()[i + 1],
        [cmd_args, conn, rets, i](EventLoop *lt, std::shared_ptr<void> arg) {
          auto db = std::static_pointer_cast<MemDB>(arg);
          if (db->Delete(conn->index(), cmd_args->args()[i + 1])) {
            rets->int_value.fetch_add(1);
          }

          rets->cnt--;
          if (rets->cnt.load() == 0) {
            conn->ReplyInteger(rets->int_value.load());
          }
        });
  }
}

void SelectCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                   std::shared_ptr<RockinConn> conn) {
  int64_t dbnum = 0;
  auto &args = cmd_args->args();
  static MemPtr g_reply_dbindex_invalid =
      rockin::make_shared<membuf_t>("ERR invalid DB index");
  static MemPtr g_reply_dbindex_range =
      rockin::make_shared<membuf_t>("ERR DB index is out of range");

  if (StringToInt64(args[1]->data, args[1]->len, &dbnum) != 1) {
    conn->ReplyError(g_reply_dbindex_invalid);
    return;
  }

  if (dbnum < 0 || dbnum >= DBNum) {
    conn->ReplyError(g_reply_dbindex_range);
    return;
  }

  conn->set_index(dbnum);
  conn->ReplyOk();
}

void FlushDBCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                    std::shared_ptr<RockinConn> conn) {
  const auto &dbs = MemSaver::Default()->dbs();
  auto rets = std::make_shared<MultiResult>(dbs.size());
  for (auto iter = dbs.begin(); iter != dbs.end(); ++iter) {
    iter->first->RunInLoopNoWait(
        [cmd_args, conn, rets](EventLoop *lt, std::shared_ptr<void> arg) {
          auto db = std::static_pointer_cast<MemDB>(arg);
          db->FlushDB(conn->index());

          rets->cnt--;
          if (rets->cnt.load() == 0) {
            conn->ReplyOk();
          }
        },
        iter->second);
  }
}

void FlushAllCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                     std::shared_ptr<RockinConn> conn) {
  const auto &dbs = MemSaver::Default()->dbs();
  auto rets = std::make_shared<MultiResult>(dbs.size());
  for (auto iter = dbs.begin(); iter != dbs.end(); ++iter) {
    iter->first->RunInLoopNoWait(
        [cmd_args, conn, rets](EventLoop *lt, std::shared_ptr<void> arg) {
          auto db = std::static_pointer_cast<MemDB>(arg);
          for (int i = 0; i < DBNum; i++) {
            db->FlushDB(i);
          }

          rets->cnt--;
          if (rets->cnt.load() == 0) {
            conn->ReplyOk();
          }
        },
        iter->second);
  }
}

void TTLCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                std::shared_ptr<RockinConn> conn) {
  MemSaver::Default()->DoCmd(
      cmd_args->args()[1], [cmd_args, conn, cmd = shared_from_this()](
                               EventLoop *lt, std::shared_ptr<void> arg) {
        auto db = std::static_pointer_cast<MemDB>(arg);
        auto &args = cmd_args->args();

        auto obj = db->Get(conn->index(), args[1]);
        if (obj == nullptr) {
          std::string meta;
          uint32_t version;
          obj = cmd->GetMeta(conn->index(), args[1], meta, version);
          if (obj == nullptr) {
            conn->ReplyInteger(-2);
            return;
          }
        }

        uint64_t cur_ms = GetMilliSec();
        if (obj->expire == 0) {
          conn->ReplyInteger(-1);
          return;
        } else if (cur_ms >= obj->expire) {
          conn->ReplyInteger(-2);
          return;
        }

        conn->ReplyInteger((obj->expire - cur_ms) / 1000);
      });
}

void PTTLCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                 std::shared_ptr<RockinConn> conn) {
  MemSaver::Default()->DoCmd(
      cmd_args->args()[1], [cmd_args, conn, cmd = shared_from_this()](
                               EventLoop *lt, std::shared_ptr<void> arg) {
        auto db = std::static_pointer_cast<MemDB>(arg);
        auto &args = cmd_args->args();

        auto obj = db->Get(conn->index(), args[1]);
        if (obj == nullptr) {
          std::string meta;
          uint32_t version;
          obj = cmd->GetMeta(conn->index(), args[1], meta, version);
          if (obj == nullptr) {
            conn->ReplyInteger(-2);
            return;
          }
        }

        uint64_t cur_ms = GetMilliSec();
        if (obj->expire == 0) {
          conn->ReplyInteger(-1);
          return;
        } else if (cur_ms >= obj->expire) {
          conn->ReplyInteger(-2);
          return;
        }

        conn->ReplyInteger(obj->expire - cur_ms);
      });
}

static bool DoExpire(std::shared_ptr<Cmd> cmd, std::shared_ptr<MemDB> db,
                     int dbindex, MemPtr key, uint64_t expire_ms) {
  std::string meta_str;
  uint32_t version;
  auto obj_meta = cmd->GetMeta(dbindex, key, meta_str, version);
  if (obj_meta == nullptr) return false;

  auto meta_ptr = rockin::make_shared<membuf_t>(meta_str);
  SET_META_EXPIRE(meta_ptr->data, expire_ms);

  auto diskdb = DiskSaver::Default()->GetDB(key);
  diskdb->SetMeta(dbindex, key, meta_ptr);

  auto obj = db->Get(dbindex, key);
  if (obj != nullptr) db->UpdateExpire(dbindex, obj, expire_ms);
  return true;
}

void ExpireCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                   std::shared_ptr<RockinConn> conn) {
  MemSaver::Default()->DoCmd(
      cmd_args->args()[1], [cmd_args, conn, cmd = shared_from_this()](
                               EventLoop *lt, std::shared_ptr<void> arg) {
        auto db = std::static_pointer_cast<MemDB>(arg);
        auto &args = cmd_args->args();

        int64_t expire_time = 0;
        if (StringToInt64(args[2]->data, args[2]->len, &expire_time) != 1) {
          conn->ReplyIntegerError();
          return;
        }

        if (expire_time <= 0)
          expire_time = GetMilliSec();
        else
          expire_time = GetMilliSec() + expire_time * 1000;

        if (!DoExpire(cmd, db, conn->index(), args[1], expire_time))
          conn->ReplyInteger(0);
        else
          conn->ReplyInteger(1);
      });
}

void PExpireCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                    std::shared_ptr<RockinConn> conn) {
  MemSaver::Default()->DoCmd(
      cmd_args->args()[1], [cmd_args, conn, cmd = shared_from_this()](
                               EventLoop *lt, std::shared_ptr<void> arg) {
        auto db = std::static_pointer_cast<MemDB>(arg);
        auto &args = cmd_args->args();

        int64_t expire_time = 0;
        if (StringToInt64(args[2]->data, args[2]->len, &expire_time) != 1) {
          conn->ReplyIntegerError();
          return;
        }

        if (expire_time <= 0)
          expire_time = GetMilliSec();
        else
          expire_time = GetMilliSec() + expire_time * 1000;

        if (!DoExpire(cmd, db, conn->index(), args[1], expire_time))
          conn->ReplyInteger(0);
        else
          conn->ReplyInteger(1);
      });
}

void ExpireAtCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                     std::shared_ptr<RockinConn> conn) {
  MemSaver::Default()->DoCmd(
      cmd_args->args()[1], [cmd_args, conn, cmd = shared_from_this()](
                               EventLoop *lt, std::shared_ptr<void> arg) {
        auto db = std::static_pointer_cast<MemDB>(arg);
        auto &args = cmd_args->args();

        int64_t expire_time = 0;
        if (StringToInt64(args[2]->data, args[2]->len, &expire_time) != 1) {
          conn->ReplyIntegerError();
          return;
        }

        expire_time = expire_time * 1000;
        uint64_t cur_time = GetMilliSec();
        if (expire_time <= cur_time) expire_time = cur_time;

        if (!DoExpire(cmd, db, conn->index(), args[1], expire_time))
          conn->ReplyInteger(0);
        else
          conn->ReplyInteger(1);
      });
}

void PExpireAtCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                      std::shared_ptr<RockinConn> conn) {
  MemSaver::Default()->DoCmd(
      cmd_args->args()[1], [cmd_args, conn, cmd = shared_from_this()](
                               EventLoop *lt, std::shared_ptr<void> arg) {
        auto db = std::static_pointer_cast<MemDB>(arg);
        auto &args = cmd_args->args();

        int64_t expire_time = 0;
        if (StringToInt64(args[2]->data, args[2]->len, &expire_time) != 1) {
          conn->ReplyIntegerError();
          return;
        }

        uint64_t cur_time = GetMilliSec();
        if (expire_time <= cur_time) expire_time = cur_time;

        if (!DoExpire(cmd, db, conn->index(), args[1], expire_time))
          conn->ReplyInteger(0);
        else
          conn->ReplyInteger(1);
      });
}

}  // namespace rockin