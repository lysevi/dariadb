#pragma once

#include <libdariadb/interfaces/idroppers.h>
#include <libdariadb/interfaces/imeasstorage.h>
#include <libdariadb/interfaces/icursor.h>
#include <libdariadb/st_exports.h>
#include <libdariadb/storage/settings.h>
#include <libdariadb/storage/wal/walfile.h>
#include <libdariadb/utils/async/locker.h>
#include <libdariadb/utils/utils.h>
#include <vector>

#include <mutex>

namespace dariadb {
namespace storage {

class WALManager;
using WALManager_ptr = std::shared_ptr<WALManager>;
class WALManager : public IMeasStorage {
public:
protected:
  EXPORT WALManager(const EngineEnvironment_ptr env);

public:
  EXPORT virtual ~WALManager();
  EXPORT static WALManager_ptr create(const EngineEnvironment_ptr env);
  // Inherited via MeasStorage
  EXPORT virtual Time minTime() override;
  EXPORT virtual Time maxTime() override;
  EXPORT virtual bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                                 dariadb::Time *maxResult) override;
  EXPORT Id2Cursor intervalReader(const QueryInterval &q)override;
  EXPORT Statistic stat(const Id id, Time from, Time to)override;
  EXPORT virtual void foreach (const QueryInterval &q,
                               IReadCallback * clbk) override;
  EXPORT virtual Id2Meas readTimePoint(const QueryTimePoint &q) override;
  EXPORT virtual Id2Meas currentValue(const IdArray &ids,
                                      const Flag &flag) override;
  EXPORT virtual Status append(const Meas &value) override;
  EXPORT virtual void flush() override;

  EXPORT std::list<std::string> closedWals();
  EXPORT void dropWAL(const std::string &fname, IWALDropper *storage);

  EXPORT size_t filesCount() const;
  EXPORT void setDownlevel(IWALDropper *down);

  EXPORT void erase(const std::string &fname);

  EXPORT void dropClosedFiles(size_t count);
  EXPORT void dropAll();

  EXPORT Id2MinMax loadMinMax() override;

protected:
  void create_new();
  std::list<std::string> wal_files() const;
  void flush_buffer();
  void drop_old_if_needed();

private:
  EXPORT static WALManager *_instance;

  WALFile_Ptr _wal;
  mutable std::mutex _locker;
  IWALDropper *_down;

  MeasArray _buffer;
  size_t _buffer_pos;
  std::set<std::string> _files_send_to_drop;
  EngineEnvironment_ptr _env;
  Settings *_settings;
};
}
}
