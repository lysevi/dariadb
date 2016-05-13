#pragma once

#include "common.h"
#include "meas.h"
#include "storage/chunk.h"
#include "storage/chunk_container.h"
#include "storage/query_param.h"
#include "utils/utils.h"
#include <memory>

namespace dariadb {
namespace storage {

class ReaderClb {
public:
  virtual void call(const Meas &m) = 0;
  virtual ~ReaderClb() {}
};

typedef std::shared_ptr<ReaderClb> ReaderClb_ptr;
class Reader;
typedef std::shared_ptr<Reader> Reader_ptr;

class Reader : public utils::NonCopy {
public:
  virtual bool isEnd() const = 0;
  virtual IdArray getIds() const = 0;
  virtual void readNext(ReaderClb *clb) = 0;
  virtual Reader_ptr clone() const = 0;
  virtual void reset() = 0; /// need call after each read operation
                            /// (readAll, readByStep, getIds...) to reset
                            /// read pos to begining

  virtual void readAll(Meas::MeasList *output);
  virtual void readAll(ReaderClb *clb);
  virtual void readByStep(ReaderClb *clb, dariadb::Time from, dariadb::Time to,
                          dariadb::Time step);
  virtual void readByStep(Meas::MeasList *output, dariadb::Time from,
                          dariadb::Time to, dariadb::Time step);

  virtual ~Reader() {}
};

class MeasSource {
public:
  virtual Time minTime() = 0;
  virtual Time maxTime() = 0;

  virtual Reader_ptr readInterval(Time from, Time to) = 0;
  virtual Reader_ptr readInTimePoint(Time time_point) = 0;
  virtual Reader_ptr readInterval(const QueryInterval &q) = 0;
  virtual Reader_ptr readInTimePoint(const QueryTimePoint &q) = 0;

  virtual Reader_ptr currentValue(const IdArray &ids, const Flag &flag) = 0;
};

class MeasWriter {
public:
  virtual append_result append(const Meas &value) = 0;

  virtual append_result append(const Meas::MeasArray &ma);
  virtual append_result append(const Meas::MeasList &ml);

  virtual void flush() = 0;
};

class MeasStorage : public utils::NonCopy,
                    public MeasSource,
                    public MeasWriter {
public:
  virtual void subscribe(const IdArray &ids, const Flag &flag,
                         const ReaderClb_ptr &clbk) = 0;
};
//
// class BaseStorage : public utils::NonCopy,
//                    public ChunkContainer,
//                    public MeasStorage {
// public:
//  virtual ~BaseStorage() = default;
//
//  /// return data in [from + to].
//  /// if 'from'> minTime return data readInTimePoint('from') +
//  /// readInterval(from,to)
//  Reader_ptr readInterval(Time from, Time to) override;
//  Reader_ptr readInTimePoint(Time time_point) override;
//  Reader_ptr readInterval(const QueryInterval &q) override;
//  Reader_ptr readInTimePoint(const QueryTimePoint &q) override;
//};

typedef std::shared_ptr<MeasSource> MeasSource_ptr;
typedef std::shared_ptr<MeasWriter> MeasWriter_ptr;
typedef std::shared_ptr<MeasStorage> MeasStorage_ptr;
// typedef std::shared_ptr<BaseStorage> BaseStorage_ptr;
}
}
