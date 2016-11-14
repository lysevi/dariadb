#pragma once

#include <cstddef>
#include <string>

namespace dariadb {
struct append_result {
  static append_result empty() { return append_result(); }
  append_result() { writed = ignored = 0; }
  append_result(size_t wr, size_t ig) {
    writed = wr;
    ignored = ig;
  }
  append_result(const append_result &other) {
    this->writed = other.writed;
    this->ignored = other.ignored;
      this->error_message=other.error_message;
  }

  append_result operator+(const append_result &other) {
    append_result res;
    res.writed = writed + other.writed;
    res.ignored = ignored + other.ignored;
    return res;
  }

  void operator=(const append_result &other) {
    this->writed = other.writed;
    this->ignored = other.ignored;
      this->error_message=other.error_message;
  }
  size_t writed;
  size_t ignored;
  std::string error_message;
};
}
