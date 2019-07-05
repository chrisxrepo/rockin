#pragma
#include <iostream>
#include <unordered_map>

namespace rockin {
class Cmd {
 public:
  virtual void Do() = 0;

  const std::string &name() { return name_; }

 private:
  std::string name_;
};

class CmdTable {
 public:
  static CmdTable *Default();

  void Init();

 private:
  std::unordered_map<std::string, Cmd *> cmd_table_;
};

}  // namespace rockin