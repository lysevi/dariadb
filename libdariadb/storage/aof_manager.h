#pragma once

#include <libdariadb/interfaces/idroppers.h>
#include <libdariadb/interfaces/imeasstorage.h>
#include <libdariadb/utils/locker.h>
#include <libdariadb/utils/utils.h>
#include <libdariadb/storage/aofile.h>
#include <vector>

#include <mutex>

namespace dariadb {
namespace storage {

class AOFManager : public IMeasStorage {
public:
protected:
  virtual ~AOFManager();

  AOFManager();

public:
  static void start();
  static void stop();
  static AOFManager *instance();

  // Inherited via MeasStorage
  virtual Time minTime() override;
  virtual Time maxTime() override;
  virtual bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                          dariadb::Time *maxResult) override;
  virtual void foreach (const QueryInterval &q, IReaderClb * clbk) override;
  virtual Id2Meas readTimePoint(const QueryTimePoint &q) override;
  virtual Id2Meas currentValue(const IdArray &ids, const Flag &flag) override;
  virtual append_result append(const Meas &value) override;
  virtual void flush() override;

  std::list<std::string> closed_aofs();
  void drop_aof(const std::string &fname, IAofDropper *storage);

  size_t files_count() const;
  void set_downlevel(IAofDropper *down);

  void erase(const std::string &fname);

  void drop_closed_files(size_t count);

protected:
  void create_new();
  std::list<std::string> aof_files() const;
  void flush_buffer();
  void drop_old_if_needed();

private:
  static AOFManager *_instance;

  AOFile_Ptr _aof;
  mutable std::mutex _locker;
  IAofDropper *_down;

  MeasArray _buffer;
  size_t _buffer_pos;
  std::set<std::string> _files_send_to_drop;
};
}
}
