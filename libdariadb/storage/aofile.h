#pragma once

#include <libdariadb/interfaces/imeasstorage.h>
#include <libdariadb/meas.h>
#include <libdariadb/dariadb_st_exports.h>
#include <memory>

namespace dariadb {
namespace storage {

const std::string AOF_FILE_EXT = ".aof"; // append-only-file
const uint64_t AOF_DEFAULT_SIZE = 1024;
class AOFile : public IMeasStorage {
public:
  DARIADB_ST_EXPORTS virtual ~AOFile();
  DARIADB_ST_EXPORTS AOFile();
  DARIADB_ST_EXPORTS AOFile(const std::string &fname, bool readonly = false);

  DARIADB_ST_EXPORTS append_result append(const Meas &value) override;
  DARIADB_ST_EXPORTS append_result append(const MeasArray::const_iterator &begin,
                       const MeasArray::const_iterator &end) override;
  DARIADB_ST_EXPORTS append_result append(const MeasList::const_iterator &begin,
                       const MeasList::const_iterator &end) override;
  DARIADB_ST_EXPORTS void foreach (const QueryInterval &q, IReaderClb * clbk) override;
  DARIADB_ST_EXPORTS Id2Meas readTimePoint(const QueryTimePoint &q) override;
  DARIADB_ST_EXPORTS Id2Meas currentValue(const IdArray &ids, const Flag &flag) override;
  DARIADB_ST_EXPORTS dariadb::Time minTime() override;
  DARIADB_ST_EXPORTS dariadb::Time maxTime() override;
  DARIADB_ST_EXPORTS bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                  dariadb::Time *maxResult) override;
  DARIADB_ST_EXPORTS void flush() override;

  DARIADB_ST_EXPORTS std::string filename() const;

  DARIADB_ST_EXPORTS std::shared_ptr<MeasArray> readAll();
  DARIADB_ST_EXPORTS static size_t writed(std::string fname);

protected:
  class Private;
  std::unique_ptr<Private> _Impl;
};

typedef std::shared_ptr<AOFile> AOFile_Ptr;
}
}
