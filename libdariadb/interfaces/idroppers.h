#pragma once

#include <string>

namespace dariadb {

class IWALDropper {
public:
  virtual void dropWAL(const std::string &fname) = 0;
  virtual ~IWALDropper() {}
};
}

