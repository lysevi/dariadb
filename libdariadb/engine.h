#pragma once

#include <libdariadb/interfaces/imeasstorage.h>
#include <libdariadb/st_exports.h>
#include <libdariadb/storage/bystep/description.h>
#include <libdariadb/storage/bystep/step_kind.h>
#include <libdariadb/storage/dropper.h>
#include <libdariadb/storage/memstorage/description.h>
#include <libdariadb/storage/settings.h>
#include <libdariadb/storage/strategy.h>
#include <libdariadb/timeutil.h>
#include <libdariadb/utils/utils.h>
#include <memory>

namespace dariadb {
namespace storage {

const uint16_t STORAGE_FORMAT = 1;
class Engine : public IMeasStorage {
public:
  struct Description {
    size_t wal_count;    ///  wal count.
    size_t pages_count;  /// pages count.
    size_t active_works; /// async tasks runned.
    bystep::Description bystep;
    Dropper::Description dropper;
    memstorage::Description memstorage;
  };

  Engine(const Engine &) = delete;
  Engine &operator=(const Engine &) = delete;
  EXPORT virtual ~Engine();

  EXPORT Engine(Settings_ptr settings, bool ignore_lock_file = false);

  using IMeasStorage::append;
  EXPORT Status append(const Meas &value) override;

  EXPORT void flush() override;
  EXPORT void stop();
  EXPORT Description description() const;

  EXPORT virtual void foreach (const QueryInterval &q, IReaderClb * clbk) override;
  EXPORT virtual MeasList readInterval(const QueryInterval &q) override;
  EXPORT virtual Id2Meas readTimePoint(const QueryTimePoint &q) override;
  EXPORT virtual Id2Meas currentValue(const IdArray &ids, const Flag &flag) override;
  EXPORT virtual void foreach (const QueryTimePoint &q, IReaderClb * clbk) override;

  EXPORT Time minTime() override;
  EXPORT Time maxTime() override;
  EXPORT bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                         dariadb::Time *maxResult) override;
  EXPORT Id2MinMax loadMinMax() override;

  EXPORT void drop_part_wals(size_t count);
  EXPORT void compress_all();

  EXPORT void subscribe(const IdArray &ids, const Flag &flag, const ReaderClb_ptr &clbk);
  EXPORT void wait_all_asyncs();

  EXPORT void fsck();

  EXPORT void eraseOld(const Time &t);

  EXPORT void compactTo(uint32_t pagesCount);
  EXPORT void compactbyTime(Time from, Time to);

  EXPORT static uint16_t format();
  EXPORT static std::string version();
  EXPORT STRATEGY strategy() const;

  EXPORT void setSteps(const Id2Step &m);

protected:
  class Private;
  std::unique_ptr<Private> _impl;
};
}
}
