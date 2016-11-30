#pragma once

#include <string>

namespace dariadb {
namespace storage {

class IAofDropper {
public:
  virtual void drop_aof(const std::string&fname) = 0;
   virtual ~IAofDropper(){}
};

}
}
