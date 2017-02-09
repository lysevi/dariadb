#pragma once

#include <libdariadb/interfaces/imeasstorage.h>
#include <libdariadb/interfaces/icursor.h>
#include <libdariadb/st_exports.h>
#include <libdariadb/storage/dropper.h>
#include <libdariadb/storage/memstorage/description.h>
#include <libdariadb/storage/settings.h>
#include <libdariadb/strategy.h>
#include <libdariadb/storage/cursors.h>
#include <libdariadb/timeutil.h>
#include <libdariadb/utils/utils.h>
#include <memory>
#include <ostream>

namespace dariadb {

const uint16_t STORAGE_FORMAT = 1;
class Engine : public IMeasStorage {
public:
  struct Description {
    size_t wal_count;    ///  wal count.
    size_t pages_count;  /// pages count.
    size_t active_works; /// async tasks runned.
	storage::Dropper::Description dropper;
	storage::memstorage::Description memstorage;
  };

  Engine(const Engine &) = delete;
  Engine &operator=(const Engine &) = delete;
  EXPORT virtual ~Engine();

  EXPORT Engine(storage::Settings_ptr settings, bool ignore_lock_file = false);

  using IMeasStorage::append;
  EXPORT Status append(const Meas &value) override;

  EXPORT void flush() override;
  EXPORT void stop();
  EXPORT Description description() const;

  EXPORT virtual void foreach (const QueryInterval &q, IReadCallback * clbk) override;
  EXPORT virtual MeasList readInterval(const QueryInterval &q) override;
  EXPORT virtual Id2Meas readTimePoint(const QueryTimePoint &q) override;
  EXPORT virtual Id2Meas currentValue(const IdArray &ids, const Flag &flag) override;
  EXPORT virtual Id2Cursor intervalReader(const QueryInterval &query)override;
  EXPORT Statistic stat(const Id id, Time from, Time to)override;
  EXPORT virtual void foreach (const QueryTimePoint &q, IReadCallback * clbk) override;

  EXPORT Time minTime() override;
  EXPORT Time maxTime() override;
  EXPORT bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                         dariadb::Time *maxResult) override;
  EXPORT Id2MinMax loadMinMax() override;

  EXPORT void drop_part_wals(size_t count);
  EXPORT void compress_all();

  EXPORT void subscribe(const IdArray &ids, const Flag &flag, const ReaderCallback_ptr &clbk);
  EXPORT void wait_all_asyncs();

  EXPORT void fsck();

  EXPORT void eraseOld(const Time &t);

  EXPORT void repack();

  EXPORT static uint16_t format();
  EXPORT static std::string version();
  EXPORT STRATEGY strategy() const;

  EXPORT void join(std::list<QueryInterval> queries, storage::Join::Callback*clbk);
protected:
  class Private;
  std::unique_ptr<Private> _impl;
};

EXPORT void row2stream(std::ostream&s, const MeasArray&row);
}
