#pragma once

#include "aof_manager.h"
#include "capacitor_manager.h"

namespace dariadb {
namespace storage {

class AofDropper : public dariadb::storage::IAofFileDropper {
public:
  AofDropper() {}
  static void drop(const AOFile_Ptr aof, const std::string &fname,
                   const std::string &storage_path);
  void drop(const std::string fname, const uint64_t aof_size) override;
  // on start, rm COLA files with name exists AOF file.
  static void cleanStorage(std::string storagePath);
};

class CapDrooper : public CapacitorManager::ICapDropper {
public:
  void drop(const std::string &fname) override;

  // on start, rm PAGE files with name exists CAP file.
  static void cleanStorage(std::string storagePath);
};
}
}
