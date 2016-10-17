#pragma once

#include <libdariadb/interfaces/imeasstorage.h>
#include <libdariadb/storage/dropper.h>
#include <libdariadb/storage/options.h>
#include <libdariadb/utils/utils.h>
#include <libdariadb/dariadb_net_ST_exports.h>
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

	DARIADBNET_ST_EXPORTS std::string to_string() const;
	DARIADBNET_ST_EXPORTS static Version from_string(const std::string &str);

	DARIADBNET_ST_EXPORTS bool operator>(const Version &other) {
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
  DARIADBNET_ST_EXPORTS virtual ~Engine();

  DARIADBNET_ST_EXPORTS Engine();

  using IMeasStorage::append;
  DARIADBNET_ST_EXPORTS append_result append(const Meas &value) override;

  DARIADBNET_ST_EXPORTS void flush() override;
  DARIADBNET_ST_EXPORTS void stop();
  DARIADBNET_ST_EXPORTS QueueSizes queue_size() const;

  DARIADBNET_ST_EXPORTS virtual void foreach (const QueryInterval &q, IReaderClb * clbk) override;
  DARIADBNET_ST_EXPORTS virtual MeasList readInterval(const QueryInterval &q) override;
  DARIADBNET_ST_EXPORTS virtual Id2Meas readTimePoint(const QueryTimePoint &q) override;
  DARIADBNET_ST_EXPORTS virtual Id2Meas currentValue(const IdArray &ids, const Flag &flag) override;
  DARIADBNET_ST_EXPORTS virtual void foreach (const QueryTimePoint &q, IReaderClb * clbk) override;

  DARIADBNET_ST_EXPORTS Time minTime() override;
  DARIADBNET_ST_EXPORTS Time maxTime() override;
  DARIADBNET_ST_EXPORTS bool minMaxTime(dariadb::Id id, dariadb::Time *minResult, dariadb::Time *maxResult) override;

  DARIADBNET_ST_EXPORTS void drop_part_aofs(size_t count);

  DARIADBNET_ST_EXPORTS void subscribe(const IdArray &ids, const Flag &flag, const ReaderClb_ptr &clbk);
  DARIADBNET_ST_EXPORTS void wait_all_asyncs();

  DARIADBNET_ST_EXPORTS void fsck();

  DARIADBNET_ST_EXPORTS Version version();
  DARIADBNET_ST_EXPORTS void eraseOld(const Time&t);
protected:
  class Private;
  std::unique_ptr<Private> _impl;
};
}
}
