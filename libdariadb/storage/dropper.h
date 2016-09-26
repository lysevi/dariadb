#pragma once

#include "../utils/locker.h"
#include "../utils/period_worker.h"
#include "aof_manager.h"

namespace dariadb {
namespace storage {

class Dropper : public dariadb::storage::IAofDropper,
                public utils::PeriodWorker {
public:
  struct Queues {
    size_t aof;
  };
  Dropper();
  ~Dropper();
  static void drop_aof(const std::string &fname, const std::string &storage_path);
  void drop_aof(const std::string fname) override;

  void flush();
  void period_call() override;
  // 1. rm PAGE files with name exists AOF file.
  static void cleanStorage(std::string storagePath);

  Queues queues() const;

private:
  void drop_aof_internal(const std::string fname);
  
  void on_period_drop_aof();
private:
  std::list<std::string> _aof_files;
  mutable utils::Locker _aof_locker;
  std::mutex _period_locker;
};
}
}
