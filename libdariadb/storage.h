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

class Reader;
typedef std::shared_ptr<Reader> Reader_ptr;

class Reader : public utils::NonCopy {
public:
  virtual bool isEnd() const = 0;
  virtual IdArray getIds() const = 0;
  virtual void readNext(ReaderClb *clb) = 0;
  virtual Reader_ptr clone() const = 0;
  virtual void reset() = 0; /// need call after each read operation
                            /// (readAll, readByStep...) to reset
                            /// read pos to begining

  virtual void readAll(Meas::MeasList *output);
  virtual void readAll(ReaderClb *clb);
  virtual void readByStep(ReaderClb *clb, dariadb::Time from, dariadb::Time to,
                          dariadb::Time step);
  virtual void readByStep(Meas::MeasList *output, dariadb::Time from, dariadb::Time to,
                          dariadb::Time step);
  virtual size_t size() = 0;
  virtual ~Reader() {}
};

class MeasSource {
public:
  virtual Time minTime() = 0;
  virtual Time maxTime() = 0;

  virtual bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                          dariadb::Time *maxResult) = 0;
  virtual void foreach(const QueryInterval&q, ReaderClb*clbk) = 0;
  virtual Meas::MeasList readInterval(const QueryInterval &q) = 0;
  virtual Meas::Id2Meas readInTimePoint(const QueryTimePoint &q) = 0;
  virtual Meas::Id2Meas currentValue(const IdArray &ids, const Flag &flag) = 0;
  virtual ~MeasSource() {}
};

class MeasWriter {
public:
  virtual append_result append(const Meas &value) = 0;
  virtual append_result append(const Meas::MeasArray &ma);
  virtual append_result append(const Meas::MeasList &ml);

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
