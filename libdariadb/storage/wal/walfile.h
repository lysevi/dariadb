#pragma once

#include <libdariadb/interfaces/icursor.h>
#include <libdariadb/interfaces/imeasstorage.h>
#include <libdariadb/meas.h>
#include <libdariadb/st_exports.h>
#include <libdariadb/storage/engine_environment.h>
#include <memory>

namespace dariadb {
namespace storage {
const std::string WAL_FILE_EXT = ".wal"; // append-only-file

class WALFile;
typedef std::shared_ptr<WALFile> WALFile_Ptr;

class WALFile : public IMeasStorage {
public:
  EXPORT virtual ~WALFile();

  EXPORT static WALFile_Ptr create(const EngineEnvironment_ptr env);
  EXPORT static WALFile_Ptr open(const EngineEnvironment_ptr env,
                                 const std::string &fname, bool readonly = false);
  EXPORT Status append(const Meas &value) override;
  EXPORT Status append(const MeasArray::const_iterator &begin,
                       const MeasArray::const_iterator &end) override;
  EXPORT Id2Cursor intervalReader(const QueryInterval &q);
  EXPORT Statistic stat(const Id id, Time from, Time to) override;
  EXPORT void foreach (const QueryInterval &q, IReadCallback * clbk) override;
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
  EXPORT Id2MinMax_Ptr loadMinMax() override;

  EXPORT Id id_bloom();
  EXPORT Id id_from_first();

  EXPORT size_t writed()const;
protected:
  EXPORT WALFile(const EngineEnvironment_ptr env);
  EXPORT WALFile(const EngineEnvironment_ptr env, const std::string &fname,
                 bool readonly);

protected:
  class Private;
  std::unique_ptr<Private> _Impl;
};
}
}
