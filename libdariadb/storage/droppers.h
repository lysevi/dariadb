#pragma once

#include "aof_manager.h"
#include "capacitor_manager.h"

namespace dariadb {
namespace storage {

class Dropper : public dariadb::storage::IAofFileDropper,public CapacitorManager::ICapDropper {
public:
  Dropper() {}
  static void drop_aof(const std::string &fname, const std::string &storage_path);
  void drop_aof(const std::string fname) override;
  void drop_cap(const std::string &fname) override;
  // 1. rm COLA files with name exists AOF file.
  // 2. rm PAGE files with name exists CAP file.
  static void cleanStorage(std::string storagePath);
};

}
}
