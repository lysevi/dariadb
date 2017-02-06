#pragma once

#include <libdariadb/meas.h>
#include <stdint.h>
namespace dariadb {
enum FLAGS : Flag {
  _NO_DATA = MAX_FLAG, // {1111 1111, 1111 1111, 1111 1111, 1111 1111}
  _REPEAT = 0x40000000 // {0100 0000, 0000 0000, 0000 0000, 0000 0000}
};
}
