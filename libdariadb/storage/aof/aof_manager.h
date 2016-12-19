#pragma once

#include <libdariadb/storage/settings.h>
#include <libdariadb/interfaces/idroppers.h>
#include <libdariadb/interfaces/imeasstorage.h>
#include <libdariadb/utils/async/locker.h>
#include <libdariadb/utils/utils.h>
#include <libdariadb/storage/aof/aofile.h>
#include <libdariadb/st_exports.h>
#include <vector>

#include <mutex>

namespace dariadb {
namespace storage {
class AOFManager : public IMeasStorage {
public:
protected:
public:
  EXPORT virtual ~AOFManager();
  EXPORT AOFManager(const EngineEnvironment_ptr env);
  // Inherited via MeasStorage
  EXPORT virtual Time minTime() override;
  EXPORT virtual Time maxTime() override;
  EXPORT virtual bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                          dariadb::Time *maxResult) override;
  EXPORT virtual void foreach (const QueryInterval &q, IReaderClb * clbk) override;
  EXPORT virtual Id2Meas readTimePoint(const QueryTimePoint &q) override;
  EXPORT virtual Id2Meas currentValue(const IdArray &ids, const Flag &flag) override;
  EXPORT virtual Status  append(const Meas &value) override;
  EXPORT virtual void flush() override;

  EXPORT std::list<std::string> closedAofs();
  EXPORT void dropAof(const std::string &fname, IAofDropper *storage);

  EXPORT size_t filesCount() const;
  EXPORT void setDownlevel(IAofDropper *down);

  EXPORT void erase(const std::string &fname);

  EXPORT void dropClosedFiles(size_t count);
  EXPORT void dropAll();

  EXPORT Id2MinMax loadMinMax() override;
protected:
  void create_new();
  std::list<std::string> aof_files() const;
  void flush_buffer();
  void drop_old_if_needed();

private:
  EXPORT static AOFManager *_instance;

  AOFile_Ptr _aof;
  mutable std::mutex _locker;
  IAofDropper *_down;

  MeasArray _buffer;
  size_t _buffer_pos;
  std::set<std::string> _files_send_to_drop;
  EngineEnvironment_ptr _env;
  Settings* _settings;
};

using AOFManager_ptr = std::shared_ptr<AOFManager>;
}
}
