#include "redis_common.h"
#include <glog/logging.h>
#include <jemalloc/jemalloc.h>
#include "conn.h"
#include "redis_cmd.h"
#include "redis_db.h"
#include "redis_pool.h"

namespace rockin {

// command
void CommandCommand(std::shared_ptr<RedisCmd> cmd) {
  cmd->ReplyOk();
  return;
}

// ping
void PingCommand(std::shared_ptr<RedisCmd> cmd) {
  static std::shared_ptr<buffer_t> g_reply_pong = make_buffer("PONG");
  cmd->ReplyString(g_reply_pong);
  return;
}

void InfoCommand(std::shared_ptr<RedisCmd> cmd) {
  size_t size = malloc_usable_size(NULL);
  std::cout << "Mem:" << size << std::endl;
  malloc_stats_print(NULL, NULL, NULL);

  cmd->ReplyOk();
}

// del key1 ...
void DelCommand(std::shared_ptr<RedisCmd> cmd) {
  int cnt = cmd->Args().size() - 1;
  auto rets = std::make_shared<MultiResult>(cnt);
  for (int i = 0; i < cnt; i++) {
    std::pair<EventLoop *, RedisDB *> db =
        RedisPool::GetInstance()->GetDB(cmd->Args()[i + 1]);

    db.first->RunInLoopNoWait([i, cmd, rets, db](EventLoop *el) {
      if (db.second->Delete(cmd->Args()[i + 1])) {
        rets->int_value.fetch_add(1);
      }

      rets->cnt--;
      if (rets->cnt.load() == 0) {
        cmd->ReplyInteger(rets->int_value.load());
      }
    });
  }
}

}  // namespace rockin