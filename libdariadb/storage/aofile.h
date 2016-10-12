#pragma once

#include "libdariadb/interfaces/imeasstorage.h"
#include "libdariadb/meas.h"
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
  append_result append(const MeasArray::const_iterator &begin,
                       const MeasArray::const_iterator &end) override;
  append_result append(const MeasList::const_iterator &begin,
                       const MeasList::const_iterator &end) override;
  void foreach (const QueryInterval &q, IReaderClb * clbk) override;
  Id2Meas readTimePoint(const QueryTimePoint &q) override;
  Id2Meas currentValue(const IdArray &ids, const Flag &flag) override;
  dariadb::Time minTime() override;
  dariadb::Time maxTime() override;
  bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                  dariadb::Time *maxResult) override;
  void flush() override;

  std::string filename() const;

  std::shared_ptr<MeasArray> readAll();
  static size_t writed(std::string fname);

protected:
  class Private;
  std::unique_ptr<Private> _Impl;
};

typedef std::shared_ptr<AOFile> AOFile_Ptr;
}
}
