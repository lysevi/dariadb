#pragma once

#include <libdariadb/meas.h>
#include <libdariadb/utils/locker.h>
#include <libdariadb/utils/thread_pool.h>
#include <libdariadb/storage/strategy.h>
#include <libdariadb/st_exports.h>

#include <string>
namespace dariadb {
namespace storage {

const std::string SETTINGS_FILE_NAME = "Settings";

class Settings {
	
public:
  EXPORT Settings(const std::string storage_path);
  EXPORT ~Settings();

  EXPORT void set_default();

  EXPORT void save();
  EXPORT void save(const std::string &file);
  EXPORT void load(const std::string &file);
  EXPORT std::vector<utils::async::ThreadPool::Params> thread_pools_params();

  EXPORT std::string dump();
  EXPORT void change(std::string& expression);
  // aof level options;
  std::string path;
  uint64_t aof_max_size;  // measurements count in one file
  size_t aof_buffer_size; // inner buffer size

  uint32_t chunk_size;

  STRATEGY strategy;

  // memstorage options;
  size_t memory_limit; //in bytes;
  float  percent_when_start_droping; //fill percent, when start dropping.
  float  percent_to_drop; //how many chunk drop.
};

using Settings_ptr = std::shared_ptr<Settings>;
}
}
