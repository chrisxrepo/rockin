#pragma once
#include <iostream>
#include <memory>
#include <unordered_map>

namespace rockin {
class CmdArgs;
class RockinConn;

struct CmdInfo {
  std::string name;
  int arity;

  CmdInfo() : arity(0) {}
  CmdInfo(std::string name_, int arity_) : name(name_), arity(arity_) {}
};

class Cmd {
 public:
  Cmd(CmdInfo info) : info_(info) {}

  virtual void Do(std::shared_ptr<CmdArgs> cmd_args,
                  std::shared_ptr<RockinConn> conn) = 0;

  const CmdInfo &info() { return info_; }

 private:
  CmdInfo info_;
};

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