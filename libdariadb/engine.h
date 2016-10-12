#pragma once

#include "libdariadb/interfaces/imeasstorage.h"
#include "libdariadb/storage/aof_manager.h"
#include "libdariadb/storage/dropper.h"
#include "libdariadb/storage/options.h"
#include "libdariadb/storage/page_manager.h"
#include "libdariadb/utils/utils.h"

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
    size_t active_works; /// async tasks runned.
    Dropper::Queues dropper_queues;
  };

  Engine(const Engine &) = delete;
  Engine &operator=(const Engine &) = delete;
  virtual ~Engine();

  Engine();

  using IMeasStorage::append;
  append_result append(const Meas &value) override;

  void flush() override;
  void stop();
  QueueSizes queue_size() const;

  virtual void foreach (const QueryInterval &q, IReaderClb * clbk) override;
  virtual MeasList readInterval(const QueryInterval &q) override;
  virtual Id2Meas readTimePoint(const QueryTimePoint &q) override;
  virtual Id2Meas currentValue(const IdArray &ids, const Flag &flag) override;
  virtual void foreach (const QueryTimePoint &q, IReaderClb * clbk) override;

  Time minTime() override;
  Time maxTime() override;
  bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                  dariadb::Time *maxResult) override;

  void drop_part_aofs(size_t count);

  void subscribe(const IdArray &ids, const Flag &flag, const ReaderClb_ptr &clbk);
  void wait_all_asyncs();

  void fsck();

  Version version();
  void eraseOld(const Time&t);
protected:
  class Private;
  std::unique_ptr<Private> _impl;
};
}
}
