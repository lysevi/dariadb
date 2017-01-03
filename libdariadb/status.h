#pragma once

#include <cstddef>
#include <string>

namespace dariadb {
struct Status {
  static Status empty() { return Status(); }
  Status() { writed = ignored = 0; }
  Status(size_t wr, size_t ig) {
    writed = wr;
    ignored = ig;
  }
  Status(const Status &other) {
    this->writed = other.writed;
    this->ignored = other.ignored;
      this->error_message=other.error_message;
  }

  Status operator+(const Status &other)const {
    Status res;
    res.writed = writed + other.writed;
    res.ignored = ignored + other.ignored;
    return res;
  }

  Status &operator=(const Status &other) {
    if (this != &other) {
      this->writed = other.writed;
      this->ignored = other.ignored;
      this->error_message = other.error_message;
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
  std::string error_message;
};
}
