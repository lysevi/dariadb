#pragma once

#include <libdariadb/interfaces/icallbacks.h>
#include <libdariadb/meas.h>
#include <libdariadb/st_exports.h>
#include <libdariadb/utils/async/locker.h>
#include <condition_variable>
#include <memory>

namespace dariadb {
namespace storage {

struct MList_ReaderClb : public IReadCallback {
  EXPORT MList_ReaderClb();
  EXPORT void apply(const Meas &m) override;

  MeasList mlist;
  utils::async::Locker _locker;
};
}
}
