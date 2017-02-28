#pragma once

#include <libdariadb/utils/exception.h>

#define NOT_IMPLEMENTED THROW_EXCEPTION("Not implemented");

#ifdef DOUBLE_CHECKS
#define ENSURE_MSG(A, E)                                                                 \
  if (!(A)) {                                                                            \
    THROW_EXCEPTION(E);                                                                  \
  }
#define ENSURE(A) ENSURE_MSG(A, #A)
#else
#define ENSURE_MSG(A)
#define ENSURE(A)
#endif

namespace dariadb {
namespace utils {

class NonCopy {
private:
  NonCopy(const NonCopy &) = delete;
  NonCopy &operator=(const NonCopy &) = delete;

protected:
  NonCopy() = default;
};
}
}
