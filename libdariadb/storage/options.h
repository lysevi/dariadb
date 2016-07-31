#pragma once

#include "../utils/locker.h"
#include "../meas.h"
#include <atomic>

namespace dariadb {
namespace storage {

const size_t AOF_BUFFER_SIZE = 1000;
const uint32_t CAP_DEFAULT_MAX_LEVELS = 11;
const uint32_t CAP_MAX_CLOSED_CAPS = 10;

class Options {
  Options() {
    aof_buffer_size = AOF_BUFFER_SIZE;
    cap_max_levels = CAP_DEFAULT_MAX_LEVELS;
    cap_max_closed_caps=CAP_MAX_CLOSED_CAPS;
    cap_store_period=0;
  }
  ~Options() = default;

public:
  static void start();
  static void stop();
  static Options *instance() { return _instance; }

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

  // aof level options;
  std::string path;
  uint64_t aof_max_size;  // measurements count in one file
  size_t aof_buffer_size; // inner buffer size

  // cap level options;
  uint32_t cap_B; // measurements count in one data block
  uint8_t  cap_max_levels;
  Time     cap_store_period;
  uint32_t cap_max_closed_caps; // if not eq 0, auto drop part of files to down-level storage
private:
  static Options *_instance;
};
}
}
