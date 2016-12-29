#pragma once

#include <libdariadb/storage/engine_environment.h>
#include <libdariadb/storage/wal/wal_manager.h>
#include <libdariadb/storage/pages/page_manager.h>
#include <string>
#include <set>
#include <mutex>

namespace dariadb {
namespace storage {

class Dropper : public dariadb::storage::IWALDropper{
public:
  struct Description {
    size_t wal;
  };
  Dropper(EngineEnvironment_ptr engine_env, PageManager_ptr page_manager, WALManager_ptr wal_manager);
  ~Dropper();
  /*static void drop_wal(const std::string &fname, const std::string &storage_path);*/
  void dropWAL(const std::string& fname) override;

  void flush();
  // 1. rm PAGE files with name exists WAL file.
  static void cleanStorage(const std::string&storagePath);

  Description description() const;
  std::mutex* getLocker() {
	  return &_dropper_lock;
  }
private:
  void drop_wal_internal(const std::string &fname);
  void write_wal_to_page(const std::string &fname, std::shared_ptr<MeasArray> ma);
private:
	std::atomic_size_t _in_queue;
	std::mutex            _queue_locker;
	std::set<std::string> _files_queue;
	PageManager_ptr _page_manager;
	WALManager_ptr _wal_manager;
	EngineEnvironment_ptr _engine_env;
	Settings* _settings;
	std::mutex _dropper_lock;
};
}
}
