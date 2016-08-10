#pragma once

#include "../common.h"
#include "../meas.h"
#include "../storage/query_param.h"
#include "icallbacks.h"
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
  virtual Meas::MeasList readInterval(const QueryInterval &q);
  virtual Meas::Id2Meas readInTimePoint(const QueryTimePoint &q) = 0;
  virtual Meas::Id2Meas currentValue(const IdArray &ids, const Flag &flag) = 0;
  virtual ~IMeasSource() {}
};

typedef std::shared_ptr<IMeasSource> IMeasSource_ptr;
}
}
