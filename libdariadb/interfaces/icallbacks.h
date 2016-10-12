#pragma once

#include <libdariadb/meas.h>
#include <memory>

namespace dariadb {
namespace storage {

class IReaderClb {
public:
  virtual void call(const Meas &m) = 0; // must be thread safety.
  virtual void is_end() = 0;            // called, when all data readed.
  virtual ~IReaderClb() {}
};

typedef std::shared_ptr<IReaderClb> ReaderClb_ptr;
}
}
