#pragma once
#include <iostream>
#include <unordered_map>
#include <vector>
#include "byte_buf.h"
#include "mem_alloc.h"
#include "utils.h"

namespace rockin {
class CmdArgs : public std::enable_shared_from_this<CmdArgs> {
 public:
  CmdArgs();

  BufPtr Parse(ByteBuf &buf);

  bool is_ok() { return args_.size() == mbulk_; }
  std::vector<BufPtr> &args() { return args_; }

  std::string ToString();

 private:
  BufPtr ParseMultiCommand(ByteBuf &buf);
  BufPtr ParseInlineCommand(ByteBuf &buf);

 private:
  std::vector<BufPtr> args_;
  int mbulk_;
};

}  // namespace rockin
