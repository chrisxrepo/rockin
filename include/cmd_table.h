#pragma once
#include <iostream>
#include <memory>
#include <unordered_map>
#include "cmd_interface.h"

namespace rockin {
class CmdTable {
 public:
  static CmdTable *Default();

  void Init();
  void HandeCmd(std::shared_ptr<RockinConn> conn,
                std::shared_ptr<CmdArgs> args);

 private:
  std::unordered_map<std::string, std::shared_ptr<Cmd>> cmd_table_;
};

}  // namespace rockin