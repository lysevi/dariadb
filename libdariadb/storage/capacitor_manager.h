#pragma once

#include "../interfaces/imeasstorage.h"
#include "../interfaces/idroppers.h"
#include "../utils/locker.h"
#include "../utils/period_worker.h"
#include "../utils/utils.h"
#include "capacitor.h"

#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace dariadb {
namespace storage {

using File2CapHeader = std::unordered_map<std::string, Capacitor::Header>;

class CapacitorManager : public IMeasStorage, protected utils::PeriodWorker {
private:
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

  static void erase(const std::string &fname);

protected:
  void drop_closed_unsafe(size_t count);
  Capacitor_Ptr create_new();
  Capacitor_Ptr create_new(std::string filename);
  std::list<std::string> cap_files() const;
  std::list<std::string>
  caps_by_filter(std::function<bool(const Capacitor::Header &)> pred);

  void period_call() override;

  void clear_files_to_send();// clean set of sended to drop files.
private:
  static CapacitorManager *_instance;

  Capacitor_Ptr _cap;
  ICapDropper *_down;
  std::unordered_set<std::string> _files_send_to_drop;

  File2CapHeader _file2header;
  utils::Locker  _cache_locker;
};
}
}
