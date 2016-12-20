#pragma once

#include <libdariadb/interfaces/imeasstorage.h>
#include <libdariadb/storage/dropper.h>
#include <libdariadb/storage/memstorage/memstorage.h>
#include <libdariadb/storage/bystep/bystep_storage.h>
#include <libdariadb/storage/settings.h>
#include <libdariadb/utils/utils.h>
#include <libdariadb/st_exports.h>
#include <libdariadb/storage/strategy.h>
#include <memory>

namespace dariadb {
namespace storage {

const uint16_t STORAGE_VERSION=1;
class Engine : public IMeasStorage {
public:
  struct Description {
    size_t aofs_count;   ///  AOF count.
    size_t pages_count;  /// pages count.
    size_t active_works; /// async tasks runned.
	ByStepStorage::Description bystep;
    Dropper::Description dropper;
	MemStorage::Description memstorage;
  };

  Engine(const Engine &) = delete;
  Engine &operator=(const Engine &) = delete;
  EXPORT virtual ~Engine();

  EXPORT Engine(Settings_ptr settings);

  using IMeasStorage::append;
  EXPORT Status  append(const Meas &value) override;

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
  EXPORT bool minMaxTime(dariadb::Id id, dariadb::Time *minResult, dariadb::Time *maxResult) override;
  EXPORT Id2MinMax loadMinMax()override;

  EXPORT void drop_part_aofs(size_t count);
  EXPORT void compress_all();

  EXPORT void subscribe(const IdArray &ids, const Flag &flag, const ReaderClb_ptr &clbk);
  EXPORT void wait_all_asyncs();

  EXPORT void fsck();

  EXPORT void eraseOld(const Time&t);

  EXPORT void compactTo(uint32_t pagesCount);
  EXPORT void compactbyTime(Time from, Time to);

  EXPORT uint16_t version();
  EXPORT STRATEGY strategy()const;
  
  ///raw.id to step.id;
  EXPORT void setId2Id(const Id2Id&m);
  EXPORT void setSteps(const Id2Step&m);
protected:
  class Private;
  std::unique_ptr<Private> _impl;
};
}
}
