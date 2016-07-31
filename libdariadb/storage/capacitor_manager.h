#pragma once

#include "../interfaces/imeasstorage.h"
#include "../utils/period_worker.h"
#include "../utils/utils.h"
#include "capacitor.h"
#include <vector>

#include <mutex>

namespace dariadb {
namespace storage {
const size_t MAX_CLOSED_CAPS = 10;
const uint8_t COLA_MAX_LEVELS = 11; //optimal value.
class CapacitorManager : public IMeasStorage, protected utils::PeriodWorker {
public:
  class ICapDropper {
  public:
    virtual void drop(const std::string &fname) = 0;
  };
protected:
  virtual ~CapacitorManager();

  CapacitorManager();

public:
  static void start();
  static void stop();
  static CapacitorManager *instance();

  // Inherited via MeasStorage
  virtual Time minTime() override;
  virtual Time maxTime() override;
  virtual bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                          dariadb::Time *maxResult) override;
  virtual void foreach (const QueryInterval &q, IReaderClb * clbk) override;
  virtual Meas::Id2Meas readInTimePoint(const QueryTimePoint &q) override;
  virtual Meas::Id2Meas currentValue(const IdArray &ids, const Flag &flag) override;
  virtual append_result append(const Meas &value) override;
  virtual void flush() override;

  void append(std::string filename, const Meas::MeasArray &ma);
  std::list<std::string> closed_caps();
  void drop_cap(const std::string &fname);

  size_t files_count() const;
  void set_downlevel(ICapDropper *down) { _down = down; }

  void fsck(bool force_check = true);   // if false - check files openned for write-only
  void drop_closed_files(size_t count); // drop 'count' closed files to down-level
                                        // storage.

protected:
  void drop_closed_unsafe(size_t count);
  Capacitor_Ptr create_new();
  Capacitor_Ptr create_new(std::string filename);
  std::list<std::string> cap_files() const;
  std::list<std::string>
  caps_by_filter(std::function<bool(const Capacitor::Header &)> pred);

  void call() override;

private:
  static CapacitorManager *_instance;

  Capacitor_Ptr _cap;
  ICapDropper *_down;
  std::set<std::string> _files_send_to_drop;
};
}
}
