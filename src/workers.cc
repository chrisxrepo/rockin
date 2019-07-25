#include "workers.h"
#include <glog/logging.h>
#include <algorithm>
#include <mutex>
#include <sstream>
#include "cmd_args.h"
#include "mem_saver.h"
#include "rockin_conn.h"
#include "siphash.h"
#include "type_control.h"
#include "type_string.h"

namespace rockin {

namespace {
std::once_flag worker_once_flag;
Workers *g_worker;
};  // namespace

Workers *Workers::Default() {
  std::call_once(worker_once_flag, []() { g_worker = new Workers(); });
  return g_worker;
}

bool Workers::Init(size_t thread_num) {
  // COMMAND
  auto command_ptr = std::make_shared<CommandCmd>(CmdInfo("command", 1));
  cmd_table_.insert(std::make_pair("command", command_ptr));

  // PING
  auto ping_ptr = std::make_shared<PingCmd>(CmdInfo("ping", 1));
  cmd_table_.insert(std::make_pair("ping", ping_ptr));

  // INFO
  auto info_ptr = std::make_shared<InfoCmd>(CmdInfo("info", 1));
  cmd_table_.insert(std::make_pair("info", info_ptr));

  // DEL key1 [key2]...
  auto del_ptr = std::make_shared<DelCmd>(CmdInfo("del", -2));
  cmd_table_.insert(std::make_pair("del", del_ptr));

  // SELECT dbnum
  auto select_ptr = std::make_shared<SelectCmd>(CmdInfo("select", 2));
  cmd_table_.insert(std::make_pair("select", select_ptr));

  // TTL key
  auto ttl_ptr = std::make_shared<TTLCmd>(CmdInfo("ttl", 2));
  cmd_table_.insert(std::make_pair("ttl", ttl_ptr));

  // PTTL key
  auto pttl_ptr = std::make_shared<PTTLCmd>(CmdInfo("pttl", 2));
  cmd_table_.insert(std::make_pair("pttl", pttl_ptr));

  // EXPIRE key seconds
  auto expire_ptr = std::make_shared<ExpireCmd>(CmdInfo("expire", 3));
  cmd_table_.insert(std::make_pair("expire", expire_ptr));

  // PEXPIRE key milliseconds
  auto pexpire_ptr = std::make_shared<PExpireCmd>(CmdInfo("pexpire", 3));
  cmd_table_.insert(std::make_pair("pexpire", pexpire_ptr));

  // EXPIREAT key timestamp
  auto expireat_ptr = std::make_shared<ExpireAtCmd>(CmdInfo("expireat", 3));
  cmd_table_.insert(std::make_pair("expireat", expireat_ptr));

  // PEXPIREAT key millisecond-timestam
  auto pexpireat_ptr = std::make_shared<PExpireAtCmd>(CmdInfo("pexpireat", 3));
  cmd_table_.insert(std::make_pair("pexpireat", pexpireat_ptr));

  // FLUSHDB
  auto flushdb_ptr = std::make_shared<FlushDBCmd>(CmdInfo("flushdb", 1));
  cmd_table_.insert(std::make_pair("flushdb", flushdb_ptr));

  // FLUSHALL
  auto flushall_ptr = std::make_shared<FlushAllCmd>(CmdInfo("flushall", 1));
  cmd_table_.insert(std::make_pair("flushall", flushall_ptr));

  // COMPACT
  auto compact_ptr = std::make_shared<CompactCmd>(CmdInfo("compact", 1));
  cmd_table_.insert(std::make_pair("compact", compact_ptr));

  // GET key
  auto get_ptr = std::make_shared<GetCmd>(CmdInfo("get", 2));
  cmd_table_.insert(std::make_pair("get", get_ptr));

  auto set_ptr = std::make_shared<SetCmd>(CmdInfo("set", -3));
  cmd_table_.insert(std::make_pair("set", set_ptr));

  // APPEND key value
  auto append_ptr = std::make_shared<AppendCmd>(CmdInfo("append", 3));
  cmd_table_.insert(std::make_pair("append", append_ptr));

  // GETSET key value
  auto getset_ptr = std::make_shared<GetSetCmd>(CmdInfo("getset", 3));
  cmd_table_.insert(std::make_pair("getset", getset_ptr));

  // MGET key1 [key2]...
  auto mget_ptr = std::make_shared<MGetCmd>(CmdInfo("mget", -2));
  cmd_table_.insert(std::make_pair("mget", mget_ptr));

  // MSET key1 value1 [kye2 value2]...
  auto mset_ptr = std::make_shared<MSetCmd>(CmdInfo("mset", -3));
  cmd_table_.insert(std::make_pair("mset", mset_ptr));

  // INCR key
  auto incr_ptr = std::make_shared<IncrCmd>(CmdInfo("incr", 2));
  cmd_table_.insert(std::make_pair("incr", incr_ptr));

  // INCRBY key value
  auto incrby_ptr = std::make_shared<IncrbyCmd>(CmdInfo("incrby", 3));
  cmd_table_.insert(std::make_pair("incrby", incrby_ptr));

  // DECR key
  auto decr_ptr = std::make_shared<DecrCmd>(CmdInfo("decr", 2));
  cmd_table_.insert(std::make_pair("decr", decr_ptr));

  // DECR key value
  auto decrby_ptr = std::make_shared<DecrbyCmd>(CmdInfo("decrby", 3));
  cmd_table_.insert(std::make_pair("decrby", decrby_ptr));

  // SETBIT key offset value
  auto setbit_ptr = std::make_shared<SetBitCmd>(CmdInfo("setbit", 4));
  cmd_table_.insert(std::make_pair("setbit", setbit_ptr));

  // GETBIT key offset
  auto getbit_ptr = std::make_shared<GetBitCmd>(CmdInfo("getbit", 3));
  cmd_table_.insert(std::make_pair("getbit", getbit_ptr));

  // BITCOUNT key [start end]
  auto bitcount_ptr = std::make_shared<BitCountCmd>(CmdInfo("bitcount", -2));
  cmd_table_.insert(std::make_pair("bitcount", bitcount_ptr));

  // BITOP AND destkey srckey1 srckey2 srckey3 ... srckeyN
  // BITOP OR destkey srckey1 srckey2 srckey3... srckeyN
  // BITOP XOR destkey srckey1 srckey2 srckey3... srckeyN
  // BITOP NOT destkey srckey
  auto bitop_ptr = std::make_shared<BitopCmd>(CmdInfo("bitop", -4));
  cmd_table_.insert(std::make_pair("bitop", bitop_ptr));

  // BITPOS key bit[start][end]
  auto bitpos_ptr = std::make_shared<BitPosCmd>(CmdInfo("bitpos", -3));
  cmd_table_.insert(std::make_pair("bitpos", bitpos_ptr));

  // STRINGDEBUG key
  auto strdebug_ptr = std::make_shared<StringDebug>(CmdInfo("strdebug", 2));
  cmd_table_.insert(std::make_pair("strdebug", strdebug_ptr));

  thread_num_ = thread_num;
  for (size_t i = 0; i < thread_num; i++)
    asyncs_.push_back(new AsyncQueue(1000));
  return this->InitAsync(thread_num);
}

void Workers::AsyncWork(int idx) {
  MemSaver::Default()->Init();

  AsyncQueue *async = asyncs_[idx];
  while (true) {
    QUEUE *q = async->Pop();
    uv__work *w = QUEUE_DATA(q, struct uv__work, wq);
    w->work(w);

    // async done
    uv_mutex_lock(&w->loop->wq_mutex);
    w->work = NULL;
    QUEUE_INSERT_TAIL(&w->loop->wq, &w->wq);
    uv_async_send(&w->loop->wq_async);
    uv_mutex_unlock(&w->loop->wq_mutex);
  }
}

void Workers::PostWork(int idx, QUEUE *q) {
  AsyncQueue *async = asyncs_[idx];
  async->Push(q);
}

void Workers::HandeCmd(std::shared_ptr<RockinConn> conn,
                       std::shared_ptr<CmdArgs> cmd_args) {
  auto &args = cmd_args->args();

  if (args.empty()) return;

  std::string cmd(args[0]->data, args[0]->len);
  std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::tolower);
  auto iter = cmd_table_.find(cmd);
  if (iter == cmd_table_.end()) {
    std::ostringstream build;
    build << "ERR unknown command `" << cmd << "`, with args beginning with: ";
    for (int i = 1; i < args.size(); i++) build << "`" << args[i] << "`, ";
    conn->ReplyError(make_buffer(build.str()));
    return;
  }

  if ((iter->second->info().arity > 0 &&
       iter->second->info().arity != args.size()) ||
      int(args.size()) < -iter->second->info().arity) {
    std::ostringstream build;
    build << "ERR wrong number of arguments for '" << cmd << "' command";
    conn->ReplyError(make_buffer(build.str()));
    return;
  }

  iter->second->Do(cmd_args, conn);
}

struct WorkHelper {
  std::shared_ptr<RockinConn> conn;
  std::function<BufPtrs()> handle;
  BufPtrs result;
};

void Workers::AsyncWork(BufPtr mkey, std::shared_ptr<RockinConn> conn,
                        std::function<BufPtrs()> handle) {
  WorkHelper *helper = new WorkHelper();
  helper->conn = conn;
  helper->handle = handle;

  uv_work_t *req = (uv_work_t *)malloc(sizeof(uv_work_t));
  req->data = helper;

  this->AsyncQueueWork(rockin::Hash(mkey->data, mkey->len) % thread_num_,
                       conn->loop(), req,
                       [](uv_work_t *req) {
                         WorkHelper *helper = (WorkHelper *)req->data;
                         helper->result = helper->handle();
                       },
                       [](uv_work_t *req, int status) {
                         WorkHelper *helper = (WorkHelper *)req->data;
                         if (helper->result.size() > 0)
                           helper->conn->WriteData(std::move(helper->result));
                         delete helper;
                         free(req);
                       });
}

struct MultiWorkData {
  std::shared_ptr<RockinConn> conn;
  std::function<ObjPtr(BufPtr)> mid_handle;
  std::function<BufPtrs(const ObjPtrs &)> handle;
  std::atomic<int> count;
  BufPtrs mkeys;
  BufPtr key;
  ObjPtrs objs;
  BufPtrs result;
};

struct MultiWorkHelper {
  int idx;
  std::shared_ptr<MultiWorkData> data;
};

void Workers::AsyncWork(BufPtrs mkeys, std::shared_ptr<RockinConn> conn,
                        std::function<ObjPtr(BufPtr)> mid_handle, BufPtr key,
                        std::function<BufPtrs(const ObjPtrs &)> handle) {
  auto data = std::make_shared<MultiWorkData>();
  data->conn = conn;
  data->mid_handle = mid_handle;
  data->handle = handle;
  data->count = mkeys.size();
  data->mkeys = mkeys;
  data->key = key;
  data->objs = ObjPtrs(mkeys.size());

  for (size_t i = 0; i < mkeys.size(); i++) {
    uv_work_t *req = (uv_work_t *)malloc(sizeof(uv_work_t));
    MultiWorkHelper *helper = new MultiWorkHelper();
    helper->idx = i;
    helper->data = data;
    req->data = helper;

    auto key = mkeys[i];
    this->AsyncQueueWork(
        rockin::Hash(key->data, key->len) % thread_num_, conn->loop(), req,
        [](uv_work_t *req) {
          MultiWorkHelper *helper = (MultiWorkHelper *)req->data;
          helper->data->objs[helper->idx] =
              helper->data->mid_handle(helper->data->mkeys[helper->idx]);
        },
        [](uv_work_t *req, int status) {
          MultiWorkHelper *helper = (MultiWorkHelper *)req->data;
          auto data = helper->data;
          delete helper;
          free(req);

          data->count--;
          if (data->count == 0) {
            uv_work_t *req = (uv_work_t *)malloc(sizeof(uv_work_t));
            MultiWorkHelper *helper = new MultiWorkHelper();
            helper->data = data;
            req->data = helper;

            Workers::Default()->AsyncQueueWork(
                rockin::Hash(data->key->data, data->key->len) %
                    Workers::Default()->thread_num_,
                data->conn->loop(), req,
                [](uv_work_t *req) {
                  MultiWorkHelper *helper = (MultiWorkHelper *)req->data;
                  helper->data->result =
                      helper->data->handle(helper->data->objs);
                },
                [](uv_work_t *req, int status) {
                  MultiWorkHelper *helper = (MultiWorkHelper *)req->data;
                  if (helper->data->result.size() > 0)
                    helper->data->conn->WriteData(
                        std::move(helper->data->result));
                  delete helper;
                  free(req);
                });
          }
        });
  }
}

}  // namespace rockin