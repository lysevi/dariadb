#pragma once

#include <libdariadb/meas.h>
#include <libdariadb/st_exports.h>
#include <condition_variable>
#include <memory>

namespace dariadb {
namespace storage {

class IReaderClb {
public:
  EXPORT	IReaderClb();
  EXPORT virtual void is_end();               // called, when all data readed.
  EXPORT virtual ~IReaderClb();
  EXPORT void wait();

  virtual void call(const Meas &m) = 0; // must be thread safety.
private:
  bool is_end_called;
  std::condition_variable _cond;
};

typedef std::shared_ptr<IReaderClb> ReaderClb_ptr;
}
}
