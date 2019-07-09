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
  static std::shared_ptr<membuf_t> g_reply_pong =
      rockin::make_shared<membuf_t>("PONG");
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
            conn->ReplyArray(rets->str_values);
          }
        });
  }
}

void SelectCmd::Do(std::shared_ptr<CmdArgs> cmd_args,
                   std::shared_ptr<RockinConn> conn) {
  int64_t dbnum = 0;
  auto &args = cmd_args->args();
  static std::shared_ptr<membuf_t> g_reply_dbindex_invalid =
      rockin::make_shared<membuf_t>("ERR invalid DB index");
  static std::shared_ptr<membuf_t> g_reply_dbindex_range =
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

}  // namespace rockin