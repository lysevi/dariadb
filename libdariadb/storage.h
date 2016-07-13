#pragma once

#include "common.h"
#include "meas.h"
#include "storage/callbacks.h"
#include "storage/chunk.h"
#include "storage/chunk_container.h"
#include "storage/query_param.h"
#include "utils/utils.h"
#include <memory>

namespace dariadb {
namespace storage {

class MeasSource {
public:
  virtual Time minTime() = 0;
  virtual Time maxTime() = 0;

  virtual bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                          dariadb::Time *maxResult) = 0;
  virtual void foreach(const QueryInterval&q, ReaderClb*clbk) = 0;
  virtual void foreach(const QueryTimePoint&q, ReaderClb*clbk);
  virtual Meas::MeasList readInterval(const QueryInterval &q) = 0;
  virtual Meas::Id2Meas readInTimePoint(const QueryTimePoint &q) = 0;
  virtual Meas::Id2Meas currentValue(const IdArray &ids, const Flag &flag) = 0;
  virtual ~MeasSource() {}
};

class MeasWriter {
public:
  virtual append_result append(const Meas &value) = 0;
  virtual append_result append(const Meas::MeasArray::const_iterator &begin,const Meas::MeasArray::const_iterator &end);
  virtual append_result append(const Meas::MeasList::const_iterator&begin,const Meas::MeasList::const_iterator&end);

  virtual void flush() = 0;
  virtual ~MeasWriter() {}
};

class MeasStorage : public utils::NonCopy, public MeasSource, public MeasWriter {
public:
};

typedef std::shared_ptr<MeasSource> MeasSource_ptr;
typedef std::shared_ptr<MeasWriter> MeasWriter_ptr;
typedef std::shared_ptr<MeasStorage> MeasStorage_ptr;
}
}
