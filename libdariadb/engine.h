#pragma once

#include <libdariadb/interfaces/imeasstorage.h>
#include <libdariadb/storage/dropper.h>
#include <libdariadb/storage/settings.h>
#include <libdariadb/utils/utils.h>
#include <libdariadb/st_exports.h>
#include <libdariadb/storage/strategy.h>
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

    EXPORT std::string to_string() const;
    EXPORT static Version from_string(const std::string &str);

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
  EXPORT virtual ~Engine();

  EXPORT Engine(Settings_ptr settings);

  using IMeasStorage::append;
  EXPORT append_result append(const Meas &value) override;

  EXPORT void flush() override;
  EXPORT void stop();
  EXPORT QueueSizes queue_size() const;

  EXPORT virtual void foreach (const QueryInterval &q, IReaderClb * clbk) override;
  EXPORT virtual MeasList readInterval(const QueryInterval &q) override;
  EXPORT virtual Id2Meas readTimePoint(const QueryTimePoint &q) override;
  EXPORT virtual Id2Meas currentValue(const IdArray &ids, const Flag &flag) override;
  EXPORT virtual void foreach (const QueryTimePoint &q, IReaderClb * clbk) override;

  EXPORT Time minTime() override;
  EXPORT Time maxTime() override;
  EXPORT bool minMaxTime(dariadb::Id id, dariadb::Time *minResult, dariadb::Time *maxResult) override;

  EXPORT void drop_part_aofs(size_t count);

  EXPORT void subscribe(const IdArray &ids, const Flag &flag, const ReaderClb_ptr &clbk);
  EXPORT void wait_all_asyncs();

  EXPORT void fsck();

  EXPORT void eraseOld(const Time&t);

  EXPORT void compactTo(uint32_t pagesCount);
  EXPORT void compactbyTime(Time from, Time to);

  EXPORT Version version();
  EXPORT STRATEGY strategy()const;
protected:
  class Private;
  std::unique_ptr<Private> _impl;
};
}
}
