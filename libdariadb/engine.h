#pragma once

#include "interfaces/imeasstorage.h"
#include "storage/aof_manager.h"
#include "storage/capacitor_manager.h"
#include "storage/dropper.h"
#include "storage/options.h"
#include "storage/page_manager.h"
#include "utils/utils.h"

#include <memory>

namespace dariadb {
namespace storage {

class Engine : public IMeasStorage {
public:
  struct Version {
    std::string version;
    uint16_t major;
    uint16_t minor;
    uint16_t patch;

    std::string to_string() const;
    static Version from_string(const std::string &str);

    bool operator>(const Version &other) {
      return (major > other.major) || (major == other.major && (minor > other.minor)) ||
             (major == other.major && (minor == other.minor) && (patch > other.patch));
    }
  };
  struct QueueSizes {
    size_t aofs_count;   ///  AOF count
    size_t pages_count;  /// pages count
    size_t cola_count;   /// COLA files count.
    size_t active_works; /// async tasks runned.
    Dropper::Queues dropper_queues;
  };

  Engine(const Engine &) = delete;
  Engine &operator=(const Engine &) = delete;
  virtual ~Engine();

  Engine();

  append_result append(const Meas &value) override;

  void flush() override;
  void stop();
  QueueSizes queue_size() const;

  virtual void foreach (const QueryInterval &q, IReaderClb * clbk) override;
  virtual Meas::MeasList readInterval(const QueryInterval &q) override;
  virtual Meas::Id2Meas readInTimePoint(const QueryTimePoint &q) override;
  virtual Meas::Id2Meas currentValue(const IdArray &ids, const Flag &flag) override;
  virtual void foreach(const QueryTimePoint &q, IReaderClb * clbk)override;

  Time minTime() override;
  Time maxTime() override;
  bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                  dariadb::Time *maxResult) override;

  Id load(const QueryInterval &qi);
  Id load(const QueryTimePoint &qt);
  Meas::MeasList getResult(Id);

  void drop_part_aofs(size_t count);
  void drop_part_caps(size_t count);

  void subscribe(const IdArray &ids, const Flag &flag, const ReaderClb_ptr &clbk);
  void wait_all_asyncs();

  void fsck();

  Version version();

protected:
  class Private;
  std::unique_ptr<Private> _impl;
};
}
}
