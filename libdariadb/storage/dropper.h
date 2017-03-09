#pragma once

#include <libdariadb/storage/dropper_description.h>
#include <libdariadb/storage/engine_environment.h>
#include <libdariadb/storage/pages/page_manager.h>
#include <libdariadb/storage/wal/wal_manager.h>
#include <condition_variable>
#include <list>
#include <mutex>
#include <string>
#include <thread>

namespace dariadb {
namespace storage {

class Dropper : public dariadb::IWALDropper {
public:
  Dropper(EngineEnvironment_ptr engine_env, PageManager_ptr page_manager,
          WALManager_ptr wal_manager);
  ~Dropper();
  void dropWAL(const std::string &fname) override;

  void flush();
  // 1. rm PAGE files with name exists WAL file.
  static void cleanStorage(const std::string &storagePath);

  DropperDescription description() const;
  std::mutex *getLocker() { return &_dropper_lock; }

private:
  void drop_wal_internal();
private:
  mutable std::mutex _queue_locker;
  std::list<std::string> _files_queue;
  bool _stop;
  bool _is_stoped;
  std::condition_variable _cond_var;
  std::thread _thread_handle;
  PageManager_ptr _page_manager;
  WALManager_ptr _wal_manager;
  EngineEnvironment_ptr _engine_env;
  Settings *_settings;
  std::mutex _dropper_lock;
  std::condition_variable _dropper_cond_var;
  int _active_operations;
};
}
}
