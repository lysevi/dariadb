#pragma once

#include <libdariadb/interfaces/icursor.h>

#include <libdariadb/engines/strategy.h>
#include <libdariadb/interfaces/iengine.h>
#include <libdariadb/st_exports.h>
#include <libdariadb/storage/cursors.h>
#include <libdariadb/storage/settings.h>
#include <libdariadb/timeutil.h>
#include <libdariadb/utils/utils.h>
#include <memory>
#include <ostream>

namespace dariadb {

const uint16_t STORAGE_FORMAT = 3;

class Engine : public IEngine {
public:
  Engine(const Engine &) = delete;
  Engine &operator=(const Engine &) = delete;
  EXPORT virtual ~Engine();

  EXPORT Engine(storage::Settings_ptr settings, bool init_threadpool = true,
                bool ignore_lock_file = false);

  using IMeasStorage::append;
  EXPORT Status append(const Meas &value) override;

  EXPORT void flush() override;
  EXPORT void stop() override;
  EXPORT IEngine::Description description() const override;

  EXPORT virtual MeasArray readInterval(const QueryInterval &q) override;
  EXPORT virtual Id2Meas readTimePoint(const QueryTimePoint &q) override;
  EXPORT virtual Id2Meas currentValue(const IdArray &ids, const Flag &flag) override;
  EXPORT virtual Id2Cursor intervalReader(const QueryInterval &query) override;
  EXPORT Statistic stat(const Id id, Time from, Time to) override;

  EXPORT Time minTime() override;
  EXPORT Time maxTime() override;
  EXPORT bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                         dariadb::Time *maxResult) override;
  EXPORT Id2MinMax_Ptr loadMinMax() override;

  EXPORT void compress_all() override;

  EXPORT void subscribe(const IdArray &ids, const Flag &flag,
                        const ReaderCallback_ptr &clbk) override;
  EXPORT void wait_all_asyncs() override;

  EXPORT void fsck() override;

  EXPORT void eraseOld(const Id id, const Time t) override;

  EXPORT void repack(dariadb::Id id) override;
  EXPORT void compact(ICompactionController *logic) override;
  EXPORT storage::Settings_ptr settings() override;
  EXPORT static uint16_t format();
  EXPORT static std::string version();
  EXPORT STRATEGY strategy() const override;

protected:
  class Private;
  std::unique_ptr<Private> _impl;
};
}
