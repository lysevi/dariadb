#pragma once

#include <libdariadb/meas.h>
#include <libdariadb/st_exports.h>

namespace dariadb {
namespace storage {

class Cola {
public:
  struct Param {
    uint8_t levels;
    uint8_t B;
    Param(const uint8_t levels_count, const uint8_t B_) {
      levels = levels_count;
      B = B_;
    }
  };

  EXPORT Cola(const Param &p, Id measId, uint8_t *buffer);
  EXPORT Cola(uint8_t *buffer);
  /// Calculate needed buffer size for store index.
  EXPORT static size_t index_size(const Param &p);
  EXPORT uint8_t levels() const;
  EXPORT Id targetId() const;

private:
  struct Private;
  std::shared_ptr<Private> _impl;
};
} // namespace storage
} // namespace dariadb