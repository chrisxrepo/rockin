#pragma once
#include <iostream>
#include <unordered_map>
#include <vector>
#include "byte_buf.h"
#include "rockin_alloc.h"
#include "utils.h"

namespace rockin {
class CmdArgs : public std::enable_shared_from_this<CmdArgs> {
 public:
  CmdArgs();

  MemPtr Parse(ByteBuf &buf);

  bool is_ok() { return args_.size() == mbulk_; }
  std::vector<MemPtr> &args() { return args_; }

  std::string ToString();

 private:
  MemPtr ParseMultiCommand(ByteBuf &buf);
  MemPtr ParseInlineCommand(ByteBuf &buf);

 private:
  std::vector<MemPtr> args_;
  int mbulk_;
};

}  // namespace rockin
