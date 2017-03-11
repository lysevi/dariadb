#pragma once

#include <cstddef>

namespace dariadb {
namespace storage {

struct DropperDescription {
  size_t wal;
  size_t active_works;
  DropperDescription() { wal = active_works = size_t(0); }
};
}
}
