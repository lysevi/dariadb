#pragma once

#include "../meas.h"
#include "../utils/locker.h"
#include "../utils/thread_pool.h"
#include "strategy.h"

namespace dariadb {
namespace storage {

const std::string OPTIONS_FILE_NAME = "Options";

class Options {
  Options();
  ~Options() = default;

public:
  static void start();
  static void start(const std::string &path);
  static void stop();
  static Options *instance() { return _instance; }

  void set_default();

  void save();
  void save(const std::string &file);
  void load(const std::string &file);
  std::vector<utils::async::ThreadPool::Params> thread_pools_params();

  // aof level options;
  std::string path;
  uint64_t aof_max_size;  // measurements count in one file
  size_t aof_buffer_size; // inner buffer size

  uint32_t page_chunk_size;
  uint32_t page_openned_page_cache_size; /// max oppend pages in cache(readonly
                                         /// pages stored).

  STRATEGY strategy;

private:
  static Options *_instance;
};
}
}
