#pragma once

#include <string>

namespace dariadb {
namespace storage {

class IWALDropper {
public:
  virtual void dropWAL(const std::string&fname) = 0;
   virtual ~IWALDropper(){}
};

}
}
