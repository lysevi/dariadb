#pragma once

#include <libdariadb/st_exports.h>
#include <libdariadb/interfaces/imeasstorage.h>
#include <libdariadb/meas.h>
#include <memory>

namespace dariadb {
namespace storage {

class MemStorage : public IMeasStorage {
public:
  struct Params {};

public:
  EXPORT MemStorage(const Params &p);
  EXPORT ~MemStorage();

  // Inherited via IMeasStorage
  EXPORT virtual Time minTime() override;
  EXPORT virtual Time maxTime() override;
  EXPORT virtual bool minMaxTime(dariadb::Id id,
                                             dariadb::Time *minResult,
                                             dariadb::Time *maxResult) override;
  EXPORT virtual void foreach (const QueryInterval &q,
                                           IReaderClb * clbk) override;
  EXPORT virtual Id2Meas readTimePoint(const QueryTimePoint &q) override;
  EXPORT virtual Id2Meas currentValue(const IdArray &ids,
                                                  const Flag &flag) override;
  using IMeasStorage::append;
  EXPORT append_result append(const Meas &value) override;
  EXPORT void flush() override;
private:
  struct Private;
  std::unique_ptr<Private> _impl;
};
}
}
