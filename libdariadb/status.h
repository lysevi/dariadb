#pragma once

#include <libdariadb/st_exports.h>
#include <cstddef>
#include <string>

namespace dariadb {

enum class APPEND_ERROR : int { OK, bad_alloc, bad_shard, wal_file_limit };

struct Status {
  static Status empty() { return Status(); }
  Status() {
    writed = ignored = 0;
    error = APPEND_ERROR::OK;
  }
  Status(size_t wr) {
    writed = wr;
    ignored = size_t(0);
    error = APPEND_ERROR::OK;
  }

  Status(size_t ig, APPEND_ERROR err) {
    ignored = ig;
    error = err;
  }

  Status(const Status &other) {
    this->writed = other.writed;
    this->ignored = other.ignored;
    this->error = other.error;
  }

  Status operator+(const Status &other) const {
    Status res;
    res.writed = writed + other.writed;
    res.ignored = ignored + other.ignored;
    return res;
  }

  Status &operator=(const Status &other) {
    if (this != &other) {
      this->writed = other.writed;
      this->ignored = other.ignored;
      this->error = other.error;
    }
    return *this;
  }

  bool operator==(const Status &other) {
    if (this != &other) {
      return this->ignored == other.ignored && this->writed == other.writed;
    }
    return true;
  }

  bool operator!=(const Status &other) {
    if (this != &other) {
      return !(*this == other);
    }
    return true;
  }
  size_t writed;
  size_t ignored;
  APPEND_ERROR error;
};

EXPORT std::string to_string(const APPEND_ERROR strat);
}
