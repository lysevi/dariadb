#pragma once

#include "../meas.h"
#include "../utils/locker.h"
#include "strategy.h"

namespace dariadb {
namespace storage {

const size_t AOF_BUFFER_SIZE = 1000;
const uint32_t CAP_DEFAULT_MAX_LEVELS = 11;
const uint32_t CAP_MAX_CLOSED_CAPS = 10;
const uint32_t OPENNED_PAGE_CACHE_SIZE = 10;
const uint32_t CHUNK_SIZE = 512;
const uint32_t CAP_B = 50;
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
  void calc_params();

  uint64_t measurements_count() const {
    return measurements_count(cap_max_levels, cap_B);
  }

  static uint64_t measurements_count(size_t levels, uint32_t B) {
    uint64_t result = 0;
    for (size_t i = 0; i < levels; ++i) {
      result += B * (uint64_t(1) << i);
    }
    return result + B; //+ memvalues size;
  }

  void save();
  void save(const std::string &file);
  void load(const std::string &file);
  // aof level options;
  std::string path;
  uint64_t aof_max_size;  // measurements count in one file
  size_t aof_buffer_size; // inner buffer size

  // cap level options;
  uint32_t cap_B; // measurements count in one data block
  uint8_t cap_max_levels;
  Time cap_store_period;
  uint32_t
      cap_max_closed_caps; // if not eq 0, auto drop part of files to down-level storage

  uint32_t page_chunk_size;
  uint32_t page_openned_page_cache_size; /// max oppend pages in cache(readonly
                                          /// pages stored).

  STRATEGY strategy;
private:
  static Options *_instance;
};
}
}
