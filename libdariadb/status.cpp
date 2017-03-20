#include <libdariadb/status.h>
#include <libdariadb/utils/exception.h>

namespace dariadb {
// TODO make normal error messages.
std::string to_string(const APPEND_ERROR st) {
  switch (st) {
  case APPEND_ERROR::OK:
    return "OK";
    break;
  case APPEND_ERROR::bad_alloc:
    return "bad_alloc";
    break;
  case APPEND_ERROR::bad_shard:
    return "bad_shard";
    break;
  case APPEND_ERROR::wal_file_limit:
    return "wal_file_limit";
    break;
  }
  THROW_EXCEPTION("std::string to_string(const APPEND_ERROR st) - ", (int)st);
}
}