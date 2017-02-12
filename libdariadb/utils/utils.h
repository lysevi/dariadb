#pragma once

#include <libdariadb/utils/exception.h>


#define NOT_IMPLEMENTED THROW_EXCEPTION("Not implemented");

#ifdef DOUBLE_CHECKS
#define ENSURE_MSG(A, E)                                                       \
  if (!(A)) {                                                                  \
    THROW_EXCEPTION(E);                                                        \
  }
#define ENSURE(A) ENSURE_MSG(A, "check failed")
#define ENSURE_NOT_NULL(A) ENSURE_MSG(A, "null pointer")
#else
#define ENSURE_MSG(A)
#define ENSURE(A)
#define ENSURE_NOT_NULL(A)
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
