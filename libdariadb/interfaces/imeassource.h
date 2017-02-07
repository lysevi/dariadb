#pragma once

#include <libdariadb/interfaces/icallbacks.h>
#include <libdariadb/interfaces/icursor.h>
#include <libdariadb/meas.h>
#include <libdariadb/st_exports.h>
#include <libdariadb/status.h>
#include <libdariadb/query_param.h>
#include <libdariadb/stat.h>
#include <memory>

namespace dariadb {

class IMeasSource {
public:
  virtual Time minTime() = 0;
  virtual Time maxTime() = 0;

  virtual bool minMaxTime(Id id, Time *minResult, Time *maxResult) = 0;
  virtual void foreach (const QueryInterval &q, IReadCallback * clbk) = 0;
  virtual void foreach (const QueryTimePoint &q, IReadCallback * clbk);
  virtual Id2Cursor intervalReader(const QueryInterval &query)=0;
  virtual Id2Meas readTimePoint(const QueryTimePoint &q) = 0;
  virtual Id2Meas currentValue(const IdArray &ids, const Flag &flag) = 0;
  virtual Statistic stat(const Id id, Time from, Time to)=0;
  EXPORT virtual Id2MinMax loadMinMax();
  EXPORT virtual MeasList readInterval(const QueryInterval &q);
  virtual ~IMeasSource() {}
};

typedef std::shared_ptr<IMeasSource> IMeasSource_ptr;
}

