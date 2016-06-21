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
#pragma pack(push, 1)
  struct Header {
    dariadb::Time minTime;
    dariadb::Time maxTime;
    bool is_closed : 1;
    bool is_full : 1;
    size_t size; // sizeof file in bytes
    size_t _writed;
    size_t _memvalues_pos;
  };
#pragma pack(pop)
  virtual ~AOFile();
  AOFile(const Params &param);
  AOFile(const AOFile::Params &params, const std::string &fname, bool readonly = false);
  // static Header readHeader(std::string file_name);
  append_result append(const Meas &value) override;
  append_result append(const Meas::MeasArray &ma) override;
  append_result append(const Meas::MeasList &ml) override;
  Reader_ptr readInterval(const QueryInterval &q) override;
  Reader_ptr readInTimePoint(const QueryTimePoint &q) override;
  Reader_ptr currentValue(const IdArray &ids, const Flag &flag) override;
  dariadb::Time minTime() override;
  dariadb::Time maxTime() override;
  bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                  dariadb::Time *maxResult) override;
  void flush() override; // write all to storage;

  void subscribe(const IdArray &, const Flag &, const ReaderClb_ptr &) override {
    NOT_IMPLEMENTED;
  }
  void drop_to_stor(MeasWriter *stor);

  std::string filename() const;

protected:
  class Private;
  std::unique_ptr<Private> _Impl;
};

typedef std::shared_ptr<AOFile> AOFile_Ptr;
}
}
