#include <assert.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <iostream>
#include "net_pool.h"
#include "redis_cmd.h"
#include "redis_dic.h"
#include "redis_pool.h"
#include "rocks_pool.h"
#include "rocksdb/db.h"
#include "server.h"

rockin::Server* g_server;

void signal_handle(uv_signal_t* handle, int signum) {
  if (signum == SIGINT) {
    LOG(INFO) << "catch signal:SIGINT";
    LOG(INFO) << "start to stop rockin...";
    rockin::NetPool::GetInstance()->Stop();
    g_server->Close();
    exit(0);
  }
  std::cout << "catch single:" << signum << std::endl;
}

void app_signal() {
  uv_signal_t* s_int = (uv_signal_t*)malloc(sizeof(uv_signal_t));
  assert(uv_signal_init(uv_default_loop(), s_int) == 0);
  uv_signal_start(s_int, signal_handle, SIGINT);

  uv_signal_t* s_user1 = (uv_signal_t*)malloc(sizeof(uv_signal_t));
  assert(uv_signal_init(uv_default_loop(), s_user1) == 0);
  uv_signal_start(s_user1, signal_handle, SIGUSR1);
}

int main(int argc, char** argv) {
  // init gflags & glog
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  // init rocksdb
  rockin::RocksPool::GetInstance()->Init(2, "/tmp/rocksdb");

  // init handle
  rockin::RedisCmd::InitHandle();

  // network thread pool
  rockin::NetPool::GetInstance()->Init(2);

  // redis work thread pool
  rockin::RedisPool::GetInstance()->Init(4);

  // listern server
  g_server = new rockin::Server;
  if (g_server->Service(9000) == false) {
    LOG(ERROR) << "start service fail";
    return -1;
  }

  // signal handle
  app_signal();

  // main thread loop
  LOG(INFO) << "start rockin success.";
  uv_run(uv_default_loop(), UV_RUN_DEFAULT);

  return 0;
}
