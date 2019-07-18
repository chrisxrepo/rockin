#include <assert.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <iostream>
#include "cmd_table.h"
#include "disk_saver.h"
#include "mem_alloc.h"
#include "mem_saver.h"
#include "rockin_server.h"
#include "rocksdb/db.h"
#include "utils.h"

void signal_handle(uv_signal_t* handle, int signum) {
  if (signum == SIGINT) {
    LOG(INFO) << "catch signal:SIGINT";
    LOG(INFO) << "start to stop rockin...";
    rockin::RockinServer::Default()->Close();
    exit(0);
  }
  std::cout << "catch single:" << signum << std::endl;
}

void init_app() {
  // sigint
  uv_signal_t* s_int = (uv_signal_t*)malloc(sizeof(uv_signal_t));
  assert(uv_signal_init(uv_default_loop(), s_int) == 0);
  uv_signal_start(s_int, signal_handle, SIGINT);

  // sigusr1
  uv_signal_t* s_user1 = (uv_signal_t*)malloc(sizeof(uv_signal_t));
  assert(uv_signal_init(uv_default_loop(), s_user1) == 0);
  uv_signal_start(s_user1, signal_handle, SIGUSR1);
}

int main(int argc, char** argv) {
  // init gflags & glog
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  rockin::MemSaver::Default()->Init(4);
  for (int i = 0; i < 10000; i++) {
    rockin::MemSaver::Default()->GetObj(uv_default_loop(), nullptr, nullptr);
  }

  /*
    // init rocksdb
    rockin::DiskSaver::Default()->InitAndCreate(1, "/tmp/rocksdb");

    // init handle
    rockin::CmdTable::Default()->Init();

    // work thread pool
    rockin::MemSaver::Default()->Init(1);

    // listern server
    rockin::RockinServer::Default()->Init(1);
    if (rockin::RockinServer::Default()->Service(9000) == false) {
      LOG(ERROR) << "start service fail";
      return -1;
    }
  */
  init_app();
  LOG(INFO) << "start rockin success.";
  uv_run(uv_default_loop(), UV_RUN_DEFAULT);

  return 0;
}
