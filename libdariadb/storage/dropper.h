#pragma once

#include <libdariadb/storage/engine_environment.h>
#include <libdariadb/storage/aof_manager.h>
#include <libdariadb/storage/page_manager.h>
#include <string>
#include <set>
#include <mutex>

namespace dariadb {
namespace storage {

class Dropper : public dariadb::storage::IAofDropper{
public:
  struct Description {
    size_t aof;
  };
  Dropper(EngineEnvironment_ptr engine_env, PageManager_ptr page_manager, AOFManager_ptr aof_manager);
  ~Dropper();
  /*static void drop_aof(const std::string &fname, const std::string &storage_path);*/
  void drop_aof(const std::string& fname) override;

  void flush();
  // 1. rm PAGE files with name exists AOF file.
  static void cleanStorage(const std::string&storagePath);

  Description description() const;

private:
  void drop_aof_internal(const std::string &fname);
  void write_aof_to_page(const std::string &fname, std::shared_ptr<MeasArray> ma);
private:
	std::atomic_size_t _in_queue;
	std::mutex            _locker;
	std::set<std::string> _addeded_files;
	PageManager_ptr _page_manager;
	AOFManager_ptr _aof_manager;
	EngineEnvironment_ptr _engine_env;
	Settings* _settings;
};
}
}
