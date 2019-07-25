#pragma once
#include <functional>
#include <iostream>
#include <memory>
#include <unordered_map>
#include "async.h"
#include "cmd_interface.h"

namespace rockin {
class Workers : public Async {
 public:
  static Workers *Default();

  bool Init(size_t thread_num);

  void HandeCmd(std::shared_ptr<RockinConn> conn,
                std::shared_ptr<CmdArgs> args);

  void AsyncWork(BufPtr mkey, std::shared_ptr<RockinConn> conn,
                 std::function<BufPtrs()> handle);

  void AsyncWork(BufPtrs mkeys, std::shared_ptr<RockinConn> conn,
                 std::function<ObjPtr(BufPtr)> mid_handle, BufPtr key,
                 std::function<BufPtrs(const ObjPtrs &)> handle);

 private:
  void AsyncWork(int idx) override;
  void PostWork(int idx, QUEUE *q) override;

 private:
  size_t thread_num_;
  std::vector<AsyncQueue *> asyncs_;
  std::unordered_map<std::string, std::shared_ptr<Cmd>> cmd_table_;
};

}  // namespace rockin