#pragma once

#include <libdariadb/meas.h>
#include <libdariadb/st_exports.h>
#include <memory>

namespace dariadb {

class IReadCallback;
typedef std::shared_ptr<IReadCallback> ReaderCallback_ptr;
class IReadCallback {
public:
  EXPORT IReadCallback();
  EXPORT virtual void is_end(); // called, when all data readed.
  EXPORT virtual ~IReadCallback();
  EXPORT void wait();
  EXPORT void cancel();            // called by user if want to stop operation.
  EXPORT bool is_canceled() const; // true - if  `cancel` was called.
  virtual void apply(const Meas &m) = 0; // must be thread safety.
private:
  bool is_end_called;
  bool is_cancel;
};

}
