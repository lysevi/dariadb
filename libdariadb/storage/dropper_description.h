#pragma once

#include <cstddef>

namespace dariadb {
namespace storage {

struct DropperDescription {
  size_t wal;
  DropperDescription() { wal = size_t(0); }
};
}
}
