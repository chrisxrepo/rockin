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
  static std::shared_ptr<buffer_t> g_reply_pong = make_buffer("PONG");
  conn->ReplyString(g_reply_pong);
}

// command
void CommandCommand(std::shared_ptr<CmdArgs> cmd) { cmd->ReplyOk(); }

// ping
void PingCommand(std::shared_ptr<CmdArgs> cmd) {
  static std::shared_ptr<buffer_t> g_reply_pong = make_buffer("PONG");
  cmd->ReplyString(g_reply_pong);
  return;
}

void InfoCommand(std::shared_ptr<CmdArgs> cmd) {
  size_t size = malloc_usable_size(NULL);
  std::cout << "Mem:" << size << std::endl;
  malloc_stats_print(NULL, NULL, NULL);

  cmd->ReplyOk();
}

// del key1 ...
void DelCommand(std::shared_ptr<CmdArgs> cmd) {
  int cnt = cmd->args().size() - 1;
  auto rets = std::make_shared<MultiResult>(cnt);
  for (int i = 0; i < cnt; i++) {
    std::pair<EventLoop *, MemDB *> loop =
        MemSaver::Default()->GetDB(cmd->args()[i + 1]);

    MemDB *db = loop.second;
    loop.first->RunInLoopNoWait(
        [i, cmd, rets, db](EventLoop *et, std::shared_ptr<void> arg) {
          if (db->Delete(cmd->DbIndex(), cmd->args()[i + 1])) {
            rets->int_value.fetch_add(1);
          }

          rets->cnt--;
          if (rets->cnt.load() == 0) {
            cmd->ReplyInteger(rets->int_value.load());
          }
        },
        nullptr);
  }
}

// select dbnum
void SelectCommand(std::shared_ptr<CmdArgs> cmd) {
  int64_t dbnum = 0;
  auto &args = cmd->args();
  if (StringToInt64(args[1]->data, args[1]->len, &dbnum) != 1) {
    cmd->ReplyError(CmdArgs::g_reply_dbindex_invalid);
    return;
  }

  if (dbnum < 0 || dbnum >= DBNum) {
    cmd->ReplyError(CmdArgs::g_reply_dbindex_range);
    return;
  }

  auto conn = cmd->conn();
  if (conn == nullptr) {
    cmd->ReplyError(make_buffer("Err conn is nullptr"));
    return;
  }

  conn->set_index(dbnum);
  cmd->ReplyOk();
}

void FlushDBCommand(std::shared_ptr<CmdArgs> cmd) {
  auto loops = MemSaver::Default()->GetDBs();
  auto rets = std::make_shared<MultiResult>(loops.size());
  for (int i = 0; i < loops.size(); i++) {
    MemDB *db = loops[i].second;
    loops[i].first->RunInLoopNoWait(
        [cmd, db, rets](EventLoop *et, std::shared_ptr<void> arg) {
          db->FlushDB(cmd->DbIndex());

          rets->cnt--;
          if (rets->cnt.load() == 0) {
            cmd->ReplyOk();
          }
        },
        nullptr);
  }
}

void FlushAllCommand(std::shared_ptr<CmdArgs> cmd) {
  auto loops = MemSaver::Default()->GetDBs();
  auto rets = std::make_shared<MultiResult>(loops.size());
  for (int i = 0; i < loops.size(); i++) {
    MemDB *db = loops[i].second;
    loops[i].first->RunInLoopNoWait(
        [cmd, db, rets](EventLoop *et, std::shared_ptr<void> arg) {
          for (int i = 0; i < DBNum; i++) {
            db->FlushDB(i);
          }

          rets->cnt--;
          if (rets->cnt.load() == 0) {
            cmd->ReplyOk();
          }
        },
        nullptr);
  }
}

}  // namespace rockin