#pragma once

#include <libdariadb/interfaces/icursor.h>
#include <libdariadb/interfaces/idroppers.h>
#include <libdariadb/interfaces/imeasstorage.h>
#include <libdariadb/st_exports.h>
#include <libdariadb/storage/settings.h>
#include <libdariadb/storage/wal/walfile.h>
#include <libdariadb/utils/async/locker.h>
#include <libdariadb/utils/striped_map.h>
#include <libdariadb/utils/utils.h>
#include <vector>

#include <mutex>
#include <shared_mutex>
namespace dariadb {
namespace storage {

class WALManager;
using WALManager_ptr = std::shared_ptr<WALManager>;
class WALManager : public IMeasStorage {
public:
  struct BufferDescription {
    WALFile_Ptr walfile;
    MeasArray buffer;
    size_t pos;
    utils::async::Locker locker;
    BufferDescription(WALFile_Ptr file, size_t buffer_size) {
      walfile = file;
      buffer.resize(buffer_size);
      pos = size_t(0);
    }
  };
  using BufferDescription_Ptr = std::shared_ptr<BufferDescription>;

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
  EXPORT Id2Cursor intervalReader(const QueryInterval &q) override;
  EXPORT Statistic stat(const Id id, Time from, Time to) override;
  EXPORT virtual void foreach (const QueryInterval &q, IReadCallback * clbk) override;
  EXPORT virtual Id2Meas readTimePoint(const QueryTimePoint &q) override;
  EXPORT virtual Id2Meas currentValue(const IdArray &ids, const Flag &flag) override;
  EXPORT virtual Status append(const Meas &value) override;
  EXPORT virtual void flush() override;

  EXPORT std::list<std::string> closedWals();
  EXPORT void dropWAL(const std::string &fname, IWALDropper *storage);

  EXPORT size_t filesCount() const;
  EXPORT void setDownlevel(IWALDropper *down);

  EXPORT void erase(const std::string &fname);

  EXPORT void dropAll();

  EXPORT Id2MinMax_Ptr loadMinMax() override;

protected:
  void dropFile(const std::string &wal);
  WALFile_Ptr create_new(BufferDescription_Ptr bd, dariadb::Id id);
  std::list<std::string> wal_files_all() const;
  std::list<std::string> wal_files(dariadb::Id id) const;
  void flush_buffer(BufferDescription_Ptr &bd, bool sync = false);
  void drop_old_if_needed();
  bool file_in_query(const std::string &filename, const QueryInterval &q);
  bool file_in_query(const std::string &filename, const QueryTimePoint &q);
  void intervalReader_async_logic(const std::list<std::string> &files,
                                  const QueryInterval &q, Id2CursorsList &readers_list,
                                  utils::async::Locker &readers_locker);

private:
  EXPORT static WALManager *_instance;

  IWALDropper *_down;

  using Id2Buffer = utils::stripped_map<dariadb::Id, BufferDescription_Ptr>;
  Id2Buffer _buffers;

  std::set<std::string> _files_send_to_drop;
  EngineEnvironment_ptr _env;
  Settings *_settings;

  struct TimeMinMax {
    Time minTime;
    Time maxTime;
    Id bloom_id;
  };
  std::unordered_map<std::string, TimeMinMax> _file2minmax;
  std::mutex _file2mm_locker;
};
} // namespace storage
} // namespace dariadb
