#pragma once

#include <string>

namespace dariadb {
namespace storage {

class IAofDropper {
public:
  virtual void dropAof(const std::string&fname) = 0;
   virtual ~IAofDropper(){}
};

}
}
