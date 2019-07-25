#pragma once
#include <vector>
#include "mem_alloc.h"

namespace rockin {

extern BufPtrs ReplyNil();

extern BufPtrs ReplyOk();

extern BufPtrs ReplyIntegerError();

extern BufPtrs ReplySyntaxError();

extern BufPtrs ReplyError(BufPtr err);

extern BufPtrs ReplyTypeError();

extern BufPtrs ReplyString(BufPtr str);

extern BufPtrs ReplyInteger(int64_t num);

extern BufPtrs ReplyBulk(BufPtr str);

extern BufPtrs ReplyArray(BufPtrs &values);

extern BufPtrs ReplyObj(std::shared_ptr<object_t> obj);
}  // namespace rockin