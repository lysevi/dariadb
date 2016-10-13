#pragma once

#include <libdariadb/append_result.h>
#include <libdariadb/meas.h>
#include <libdariadb/storage/query_param.h>
#include <libdariadb/interfaces/icallbacks.h>
#include <memory>

namespace dariadb {
namespace storage {

class IMeasSource {
public:
  virtual Time minTime() = 0;
  virtual Time maxTime() = 0;

  virtual bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                          dariadb::Time *maxResult) = 0;
  virtual void foreach (const QueryInterval &q, IReaderClb * clbk) = 0;
  virtual void foreach (const QueryTimePoint &q, IReaderClb * clbk);
  virtual MeasList readInterval(const QueryInterval &q);
  virtual Id2Meas readTimePoint(const QueryTimePoint &q) = 0;
  virtual Id2Meas currentValue(const IdArray &ids, const Flag &flag) = 0;
  virtual ~IMeasSource() {}
};

typedef std::shared_ptr<IMeasSource> IMeasSource_ptr;
}
}
