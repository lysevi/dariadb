#pragma once

#include "../storage.h"
#include "../utils/utils.h"
#include "capacitor.h"
#include <vector>

#include <mutex>

namespace dariadb {
namespace storage {

class CapacitorManager : public MeasStorage {
public:
  struct Params {
    std::string path;
    size_t max_levels;
    size_t B; // measurements count in one data block
    Params() {
      max_levels = 0;
      B = 0;
    }
    Params(const std::string storage_path, const size_t _B) {
      path = storage_path;
      B = _B;
      max_levels = 0;
    }


	size_t measurements_count()const {
		size_t result=0;
		for (size_t i = 0; i < max_levels; ++i) {
			result += B*(size_t(1) << i);
		}
		return result;
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
  virtual Reader_ptr readInterval(const QueryInterval &q) override;
  virtual Reader_ptr readInTimePoint(const QueryTimePoint &q) override;
  virtual Reader_ptr currentValue(const IdArray &ids, const Flag &flag) override;
  virtual append_result append(const Meas &value) override;
  virtual void flush() override;
  virtual void subscribe(const IdArray &ids, const Flag &flag,
                         const ReaderClb_ptr &clbk) override;

  std::list<std::string> closed_caps();
  void drop_cap(const std::string &fname, MeasWriter *storage);

  size_t files_count() const;
  void set_downlevel(MeasWriter *down) { _down = down; }

protected:
  void create_new();
  std::list<std::string> cap_files() const;
  std::list<std::string>
  caps_by_filter(std::function<bool(const Capacitor::Header &)> pred);

private:
  static CapacitorManager *_instance;

  Params _params;
  Capacitor_Ptr _cap;
  mutable std::mutex _locker;
  MeasWriter *_down;
};
}
}
