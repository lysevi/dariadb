#pragma once

#include <string>

namespace dariadb {
namespace storage {

class IAofDropper {
public:
  virtual void drop_aof(const std::string fname) = 0;
};

class ICapDropper {
public:
  virtual void drop_cap(const std::string &fname) = 0;
};
}
}
