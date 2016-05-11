#pragma once

#include "../meas.h"
#include "../storage.h"
#include "../utils/period_worker.h"
#include <memory>

namespace dariadb {
namespace storage {
/// used as added period in PeriodWorker
	const std::string CAP_FILE_EXT=".aof";
class Capacitor : public utils::NonCopy, public MeasStorage {
public:
	
  struct Params {
    size_t B;
	std::string path;
    Params(const size_t _B, const std::string _path) {
      B = _B;
      path=_path;
    }
  };
  virtual ~Capacitor();
  Capacitor(const BaseStorage_ptr stor, const Params &param);

  append_result append(const Meas &value) override;
  virtual Reader_ptr readInterval(Time from, Time to) override;
  virtual Reader_ptr readInTimePoint(Time time_point) override;
  virtual Reader_ptr readInterval(const IdArray &ids, Flag flag, Time from,
                                  Time to) override;
  virtual Reader_ptr readInTimePoint(const IdArray &ids, Flag flag,
                                     Time time_point) override;

  dariadb::Time minTime() override;
  dariadb::Time maxTime() override;

  bool flush(); // write all to storage;

  size_t in_queue_size()const;
protected:
  class Private;
  std::unique_ptr<Private> _Impl;

  // Inherited via MeasStorage
};
}
}
