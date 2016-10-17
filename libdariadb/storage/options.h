#pragma once

#include <libdariadb/meas.h>
#include <libdariadb/utils/locker.h>
#include <libdariadb/utils/thread_pool.h>
#include <libdariadb/storage/strategy.h>
#include <libdariadb/dariadb_st_exports.h>

namespace dariadb {
namespace storage {

const std::string OPTIONS_FILE_NAME = "Options";

class Options {
  Options();
  ~Options() = default;

public:
  DARIADB_ST_EXPORTS static void start();
  DARIADB_ST_EXPORTS static void start(const std::string &path);
  DARIADB_ST_EXPORTS static void stop();
  static Options *instance() { return _instance; }

  DARIADB_ST_EXPORTS void set_default();

  DARIADB_ST_EXPORTS void save();
  DARIADB_ST_EXPORTS void save(const std::string &file);
  DARIADB_ST_EXPORTS void load(const std::string &file);
  DARIADB_ST_EXPORTS std::vector<utils::async::ThreadPool::Params> thread_pools_params();

  // aof level options;
  std::string path;
  uint64_t aof_max_size;  // measurements count in one file
  size_t aof_buffer_size; // inner buffer size

  uint32_t page_chunk_size;
  uint32_t page_openned_page_cache_size; /// max oppend pages in cache(readonly
                                         /// pages stored).

  STRATEGY strategy;

private:
  DARIADB_ST_EXPORTS static Options *_instance;
};
}
}
