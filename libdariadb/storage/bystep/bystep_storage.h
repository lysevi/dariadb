#pragma once

#include <libdariadb/interfaces/imeasstorage.h>
#include <libdariadb/st_exports.h>
#include <libdariadb/storage/engine_environment.h>
#include <libdariadb/storage/memstorage/allocators.h>
#include <memory>

namespace dariadb {
namespace storage {

class ByStepStorage : public IMeasStorage {
public:
  /// id_count - for prealloc
  EXPORT ByStepStorage(const EngineEnvironment_ptr &env);
  EXPORT ~ByStepStorage();

  // Inherited via IMeasStorage
  EXPORT virtual Time minTime() override;
  EXPORT virtual Time maxTime() override;
  EXPORT virtual bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                                 dariadb::Time *maxResult) override;
  EXPORT virtual void foreach (const QueryInterval &q, IReaderClb * clbk) override;
  EXPORT virtual Id2Meas readTimePoint(const QueryTimePoint &q) override;
  EXPORT virtual Id2Meas currentValue(const IdArray &ids, const Flag &flag) override;
  using IMeasStorage::append;
  EXPORT Status append(const Meas &value) override;
  EXPORT void flush() override;
  EXPORT void stop();
  EXPORT Id2MinMax loadMinMax() override;
private:
  struct Private;
  std::unique_ptr<Private> _impl;
};
using ByStepStorage_ptr = std::shared_ptr<ByStepStorage>;
}
}
