#pragma once

#include <libdariadb/storage/engine_environment.h>
#include <libdariadb/interfaces/imeasstorage.h>
#include <libdariadb/meas.h>
#include <libdariadb/st_exports.h>
#include <memory>

namespace dariadb {
namespace storage {

const std::string AOF_FILE_EXT = ".aof"; // append-only-file
const uint64_t AOF_DEFAULT_SIZE = 1024;
class AOFile : public IMeasStorage {
public:
  EXPORT virtual ~AOFile();
  EXPORT AOFile(const EngineEnvironment_ptr env);
  EXPORT AOFile(const EngineEnvironment_ptr env, const std::string &fname, bool readonly = false);

  EXPORT Status  append(const Meas &value) override;
  EXPORT Status  append(const MeasArray::const_iterator &begin,
                       const MeasArray::const_iterator &end) override;
  EXPORT Status  append(const MeasList::const_iterator &begin,
                       const MeasList::const_iterator &end) override;
  EXPORT void foreach (const QueryInterval &q, IReaderClb * clbk) override;
  EXPORT Id2Meas readTimePoint(const QueryTimePoint &q) override;
  EXPORT Id2Meas currentValue(const IdArray &ids, const Flag &flag) override;
  EXPORT dariadb::Time minTime() override;
  EXPORT dariadb::Time maxTime() override;
  EXPORT bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                  dariadb::Time *maxResult) override;
  EXPORT void flush() override;

  EXPORT std::string filename() const;

  EXPORT std::shared_ptr<MeasArray> readAll();
  EXPORT static size_t writed(std::string fname);
  EXPORT Id2MinMax loadMinMax() override;
protected:
  class Private;
  std::unique_ptr<Private> _Impl;
};

typedef std::shared_ptr<AOFile> AOFile_Ptr;
}
}
