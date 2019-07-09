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

  std::shared_ptr<membuf_t> Parse(ByteBuf &buf);

  bool is_ok() { return args_.size() == mbulk_; }
  std::vector<std::shared_ptr<membuf_t>> &args() { return args_; }

  std::string ToString();

 private:
  std::shared_ptr<membuf_t> ParseMultiCommand(ByteBuf &buf);
  std::shared_ptr<membuf_t> ParseInlineCommand(ByteBuf &buf);

 private:
  std::vector<std::shared_ptr<membuf_t>> args_;
  int mbulk_;
};

}  // namespace rockin
