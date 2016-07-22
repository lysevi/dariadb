#pragma once

#include "../meas.h"
#include <memory>

namespace dariadb {
namespace storage {

class IReaderClb {
public:
  virtual void call(const Meas &m) = 0; // must be thread safety.
  virtual ~IReaderClb() {}
};

typedef std::shared_ptr<IReaderClb> ReaderClb_ptr;

}
}
