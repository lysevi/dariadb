#pragma once

#include "../storage.h"
#include "../utils/period_worker.h"
#include "../utils/utils.h"
#include "capacitor.h"
#include <vector>

#include <mutex>

namespace dariadb {
namespace storage {
const size_t MAX_CLOSED_CAPS = 10;

class CapacitorManager : public MeasStorage, protected utils::PeriodWorker {
public:
  class CapDropper {
  public:
    virtual void drop(const std::string &fname) = 0;
  };
  struct Params {
    std::string path;
    size_t max_levels;
    size_t max_closed_caps; // if not eq 0, auto drop part of files to down-level storage
    size_t B;               // measurements count in one data block
    dariadb::Time store_period;
    Params() {
      max_levels = 0;
      B = 0;
      store_period = 0;
    }
    Params(const std::string storage_path, const size_t _B) {
      path = storage_path;
      B = _B;
      max_levels = 0;
      max_closed_caps = MAX_CLOSED_CAPS;
      store_period = 0;
    }

    size_t measurements_count() const {
      return Capacitor::Params::measurements_count(max_levels, B);
    }
  };

protected:
  virtual ~CapacitorManager();

  CapacitorManager(const Params &param);

public:
  static void start(const Params &param);
  static void stop();
  static CapacitorManager *instance();

  // Inherited via MeasStorage
  virtual Time minTime() override;
  virtual Time maxTime() override;
  virtual bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                          dariadb::Time *maxResult) override;
  virtual void foreach (const QueryInterval &q, ReaderClb * clbk) override;
  virtual Meas::MeasList readInterval(const QueryInterval &q) override;
  virtual Meas::Id2Meas readInTimePoint(const QueryTimePoint &q) override;
  virtual Meas::Id2Meas currentValue(const IdArray &ids, const Flag &flag) override;
  virtual append_result append(const Meas &value) override;
  virtual void flush() override;

  void append(std::string filename, const Meas::MeasArray &ma);
  std::list<std::string> closed_caps();
  void drop_cap(const std::string &fname);

  size_t files_count() const;
  void set_downlevel(CapDropper *down) { _down = down; }

  void fsck(bool force_check = true); // if false - check files openned for write-only
  void drop_part(size_t count);

protected:
  void drop_part_unsafe(size_t count);
  Capacitor_Ptr create_new();
  Capacitor_Ptr create_new(std::string filename);
  std::list<std::string> cap_files() const;
  std::list<std::string>
  caps_by_filter(std::function<bool(const Capacitor::Header &)> pred);

  void call() override;

private:
  static CapacitorManager *_instance;

  Params _params;
  Capacitor_Ptr _cap;
  mutable std::mutex _locker;
  CapDropper *_down;
  std::set<std::string> _files_send_to_drop;
};
}
}
