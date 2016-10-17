#pragma once

#include <libdariadb/interfaces/imeasstorage.h>
#include <libdariadb/storage/dropper.h>
#include <libdariadb/storage/options.h>
#include <libdariadb/utils/utils.h>
#include <libdariadb/dariadb_st_exports.h>
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

    DARIADB_ST_EXPORTS std::string to_string() const;
    DARIADB_ST_EXPORTS static Version from_string(const std::string &str);

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
  DARIADB_ST_EXPORTS virtual ~Engine();

  DARIADB_ST_EXPORTS Engine();

  using IMeasStorage::append;
  DARIADB_ST_EXPORTS append_result append(const Meas &value) override;

  DARIADB_ST_EXPORTS void flush() override;
  DARIADB_ST_EXPORTS void stop();
  DARIADB_ST_EXPORTS QueueSizes queue_size() const;

  DARIADB_ST_EXPORTS virtual void foreach (const QueryInterval &q, IReaderClb * clbk) override;
  DARIADB_ST_EXPORTS virtual MeasList readInterval(const QueryInterval &q) override;
  DARIADB_ST_EXPORTS virtual Id2Meas readTimePoint(const QueryTimePoint &q) override;
  DARIADB_ST_EXPORTS virtual Id2Meas currentValue(const IdArray &ids, const Flag &flag) override;
  DARIADB_ST_EXPORTS virtual void foreach (const QueryTimePoint &q, IReaderClb * clbk) override;

  DARIADB_ST_EXPORTS Time minTime() override;
  DARIADB_ST_EXPORTS Time maxTime() override;
  DARIADB_ST_EXPORTS bool minMaxTime(dariadb::Id id, dariadb::Time *minResult, dariadb::Time *maxResult) override;

  DARIADB_ST_EXPORTS void drop_part_aofs(size_t count);

  DARIADB_ST_EXPORTS void subscribe(const IdArray &ids, const Flag &flag, const ReaderClb_ptr &clbk);
  DARIADB_ST_EXPORTS void wait_all_asyncs();

  DARIADB_ST_EXPORTS void fsck();

  DARIADB_ST_EXPORTS Version version();
  DARIADB_ST_EXPORTS void eraseOld(const Time&t);
protected:
  class Private;
  std::unique_ptr<Private> _impl;
};
}
}
