#include <libdariadb/status.h>
#include <libdariadb/utils/exception.h>

namespace dariadb {
std::string to_string(const APPEND_ERROR st) {
  switch (st) {
  case APPEND_ERROR::OK:
    return "OK";
    break;
  case APPEND_ERROR::bad_alloc:
    return "memory allocation error";
    break;
  case APPEND_ERROR::bad_shard:
    return "shard not found";
    break;
  case APPEND_ERROR::wal_file_limit:
    return "wall file is full";
    break;
  }
  THROW_EXCEPTION("std::string to_string(const APPEND_ERROR st) - ", (int)st);
}
}