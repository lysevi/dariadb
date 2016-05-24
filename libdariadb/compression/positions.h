#pragma once
#include "../meas.h"

namespace dariadb {
namespace compression {

struct IdCompressionPosition {
  bool _is_first;
  Id _first;
};

struct FlagCompressionPosition {
  bool _is_first;
  Flag _first;
};

struct DeltaCompressionPosition {
  bool _is_first;
  Time _first;
  uint64_t _prev_delta;
  Time _prev_time;
};

struct XorCompressionPosition {
  bool _is_first;
  uint64_t _first;
  uint64_t _prev_value;
  uint8_t _prev_lead;
  uint8_t _prev_tail;
};
}
}
