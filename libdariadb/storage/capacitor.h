#pragma once

#include "../meas.h"
#include "../storage.h"
#include "../utils/period_worker.h"
#include <memory>

namespace dariadb {
namespace storage {
/// used as added period in PeriodWorker
const dariadb::Time capasitor_sync_delta = 500;
class Capacitor : public utils::NonCopy,
                  protected dariadb::utils::PeriodWorker {
public:
  struct Params {
    size_t max_size;
    dariadb::Time write_window_deep;
    Params(const size_t maximum_size, const dariadb::Time write_window) {
      max_size = maximum_size;
      write_window_deep = write_window;
    }
  };
  ~Capacitor();
  Capacitor(const BaseStorage_ptr stor, const Params &param);

  bool append(const Meas &m);
  size_t size() const;
  dariadb::Time minTime() const;
  dariadb::Time maxTime() const;
  size_t writed_count() const;
  bool flush(); // write all to storage;
  void clear();

protected:
  class Private;
  std::unique_ptr<Private> _Impl;

  // Inherited via PeriodWorker
  virtual void call() override;
};
}
}
