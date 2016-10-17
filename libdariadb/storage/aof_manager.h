#pragma once

#include <libdariadb/interfaces/idroppers.h>
#include <libdariadb/interfaces/imeasstorage.h>
#include <libdariadb/utils/locker.h>
#include <libdariadb/utils/utils.h>
#include <libdariadb/storage/aofile.h>
#include <libdariadb/dariadb_st_exports.h>
#include <vector>

#include <mutex>

namespace dariadb {
namespace storage {

class AOFManager : public IMeasStorage {
public:
protected:
  DARIADB_ST_EXPORTS virtual ~AOFManager();

  DARIADB_ST_EXPORTS AOFManager();

public:
  DARIADB_ST_EXPORTS static void start();
  DARIADB_ST_EXPORTS static void stop();
  DARIADB_ST_EXPORTS static AOFManager *instance();

  // Inherited via MeasStorage
  DARIADB_ST_EXPORTS virtual Time minTime() override;
  DARIADB_ST_EXPORTS virtual Time maxTime() override;
  DARIADB_ST_EXPORTS virtual bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                          dariadb::Time *maxResult) override;
  DARIADB_ST_EXPORTS virtual void foreach (const QueryInterval &q, IReaderClb * clbk) override;
  DARIADB_ST_EXPORTS virtual Id2Meas readTimePoint(const QueryTimePoint &q) override;
  DARIADB_ST_EXPORTS virtual Id2Meas currentValue(const IdArray &ids, const Flag &flag) override;
  DARIADB_ST_EXPORTS virtual append_result append(const Meas &value) override;
  DARIADB_ST_EXPORTS virtual void flush() override;

  DARIADB_ST_EXPORTS std::list<std::string> closed_aofs();
  DARIADB_ST_EXPORTS void drop_aof(const std::string &fname, IAofDropper *storage);

  DARIADB_ST_EXPORTS size_t files_count() const;
  DARIADB_ST_EXPORTS void set_downlevel(IAofDropper *down);

  DARIADB_ST_EXPORTS void erase(const std::string &fname);

  DARIADB_ST_EXPORTS void drop_closed_files(size_t count);

protected:
  void create_new();
  std::list<std::string> aof_files() const;
  void flush_buffer();
  void drop_old_if_needed();

private:
  DARIADB_ST_EXPORTS static AOFManager *_instance;

  AOFile_Ptr _aof;
  mutable std::mutex _locker;
  IAofDropper *_down;

  MeasArray _buffer;
  size_t _buffer_pos;
  std::set<std::string> _files_send_to_drop;
};
}
}
