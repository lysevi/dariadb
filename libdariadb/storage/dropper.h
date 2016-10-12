#pragma once

#include "libdariadb/storage/aof_manager.h"
#include <string>
#include <set>
#include <mutex>

namespace dariadb {
namespace storage {

class Dropper : public dariadb::storage::IAofDropper{
public:
  struct Queues {
    size_t aof;
  };
  Dropper();
  ~Dropper();
  static void drop_aof(const std::string &fname, const std::string &storage_path);
  void drop_aof(const std::string fname) override;

  void flush();
  // 1. rm PAGE files with name exists AOF file.
  static void cleanStorage(std::string storagePath);

  Queues queues() const;

private:
  void drop_aof_internal(const std::string fname);
  void write_aof_to_page(const std::string fname, std::shared_ptr<MeasArray> ma);
private:
	std::atomic_int _in_queue;
	std::mutex            _locker;
	std::set<std::string> _addeded_files;
};
}
}
