#pragma once

#include <libdariadb/dariadb_st_exports.h>
#include <libdariadb/interfaces/imeassource.h>
#include <libdariadb/meas.h>
#include <memory>

namespace dariadb {
namespace storage {
class MemStorage : public IMeasSource {
public:
  struct Params {};

public:
  DARIADB_ST_EXPORTS MemStorage(const Params &p);
  DARIADB_ST_EXPORTS ~MemStorage();

  // Inherited via IMeasStorage
  DARIADB_ST_EXPORTS virtual Time minTime() override;
  DARIADB_ST_EXPORTS virtual Time maxTime() override;
  DARIADB_ST_EXPORTS virtual bool minMaxTime(dariadb::Id id,
                                             dariadb::Time *minResult,
                                             dariadb::Time *maxResult) override;
  DARIADB_ST_EXPORTS virtual void foreach (const QueryInterval &q,
                                           IReaderClb * clbk) override;
  DARIADB_ST_EXPORTS virtual Id2Meas
  readTimePoint(const QueryTimePoint &q) override;
  DARIADB_ST_EXPORTS virtual Id2Meas currentValue(const IdArray &ids,
                                                  const Flag &flag) override;
  DARIADB_ST_EXPORTS append_result append(const Time &step,
                                          const MeasArray &values);

private:
  struct Private;
  std::unique_ptr<Private> _impl;
};
}
}
