#pragma once

#include <libdariadb/meas.h>
#include <libdariadb/st_exports.h>

namespace dariadb {
namespace storage {

class Cola {
public:
  struct Param {
    size_t levels;
    size_t B;
    Param(const size_t levels_count, const size_t B_) {
      levels = levels_count;
      B = B_;
    }
  };

  EXPORT Cola(const Param &p, const uint8_t *buffer);
  /// Calculate needed buffer size for store index.
  EXPORT static size_t index_size(const Param &p);

private:
  struct Private;
  std::shared_ptr<Private> _impl;
};
} // namespace storage
} // namespace dariadb