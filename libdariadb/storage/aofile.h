#pragma once

#include "../meas.h"
#include "../storage.h"
#include <memory>

namespace dariadb {
namespace storage {

const std::string AOF_FILE_EXT = ".aof"; // append-only-file

class AOFile : public MeasStorage {
public:
  struct Params {
    size_t size; // measurements count
    std::string path;
    Params(const size_t _size, const std::string _path) {
      size = _size;
      path = _path;
    }
  };
  virtual ~AOFile();
  AOFile(const Params &param);
  AOFile(const AOFile::Params &params, const std::string &fname, bool readonly = false);
  // static Header readHeader(std::string file_name);
  append_result append(const Meas &value) override;
  append_result append(const Meas::MeasArray::const_iterator &begin,const Meas::MeasArray::const_iterator &end) override;
  append_result append(const Meas::MeasList::const_iterator&begin,const Meas::MeasList::const_iterator&end) override;
  void foreach(const QueryInterval&q, ReaderClb*clbk) override;
  Meas::MeasList readInterval(const QueryInterval &q) override;
  Meas::Id2Meas readInTimePoint(const QueryTimePoint &q) override;
  Meas::Id2Meas currentValue(const IdArray &ids, const Flag &flag) override;
  dariadb::Time minTime() override;
  dariadb::Time maxTime() override;
  bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                  dariadb::Time *maxResult) override;
  void flush() override; // write all to storage;

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
