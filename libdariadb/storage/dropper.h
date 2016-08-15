#pragma once

#include "../utils/locker.h"
#include "../utils/period_worker.h"
#include "aof_manager.h"
#include "capacitor_manager.h"

namespace dariadb {
namespace storage {

class Dropper : public dariadb::storage::IAofDropper,
                public dariadb::storage::ICapDropper,
                public utils::PeriodWorker{
public:
    struct Queues{
      size_t aof;
      size_t cap;
    };
  Dropper();
  ~Dropper();
  static void drop_aof(const std::string &fname,
                       const std::string &storage_path);
  void drop_aof(const std::string fname) override;
  void drop_cap(const std::string &fname) override;

  void flush();
  void period_call()override;
  // 1. rm COLA files with name exists AOF file.
  // 2. rm PAGE files with name exists CAP file.
  static void cleanStorage(std::string storagePath);

  Queues queues()const;
private:
  void drop_aof_internal(const std::string fname);
  void drop_cap_internal(const std::string &fname);
  void drop_aof_to_compress(const std::string &fname);
private:
  std::list<std::string> _aof_files;
  std::list<std::string> _cap_files;
  mutable utils::Locker _locker;
  std::mutex _period_locker;
};
}
}
