#pragma once

#include <libdariadb/interfaces/icallbacks.h>
#include <libdariadb/meas.h>
#include <libdariadb/utils/locker.h>
#include <libdariadb/st_exports.h>
#include <condition_variable>
#include <memory>

namespace dariadb {
namespace storage {

struct MList_ReaderClb : public IReaderClb {
  EXPORT MList_ReaderClb();
  EXPORT void call(const Meas &m) override;
  EXPORT void is_end() override;
  EXPORT void wait();

  MeasList mlist;
  utils::Locker _locker;
  bool is_end_called;
  std::condition_variable _cond;
};
}
}
