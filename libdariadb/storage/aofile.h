#pragma once

#include "../interfaces/imeasstorage.h"
#include "../meas.h"
#include <memory>

namespace dariadb {
namespace storage {

const std::string AOF_FILE_EXT = ".aof"; // append-only-file
const uint64_t AOF_DEFAULT_SIZE = 1024;
class AOFile : public IMeasStorage {
public:
  virtual ~AOFile();
  AOFile();
  AOFile(const std::string &fname, bool readonly = false);

  append_result append(const Meas &value) override;
  append_result append(const Meas::MeasArray::const_iterator &begin,
                       const Meas::MeasArray::const_iterator &end) override;
  append_result append(const Meas::MeasList::const_iterator &begin,
                       const Meas::MeasList::const_iterator &end) override;
  void foreach (const QueryInterval &q, IReaderClb * clbk) override;
  Meas::Id2Meas readInTimePoint(const QueryTimePoint &q) override;
  Meas::Id2Meas currentValue(const IdArray &ids, const Flag &flag) override;
  dariadb::Time minTime() override;
  dariadb::Time maxTime() override;
  bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                  dariadb::Time *maxResult) override;
  void flush() override;

  std::string filename() const;

  Meas::MeasArray readAll();
  static size_t writed(std::string fname);

protected:
  class Private;
  std::unique_ptr<Private> _Impl;
};

typedef std::shared_ptr<AOFile> AOFile_Ptr;
}
}
