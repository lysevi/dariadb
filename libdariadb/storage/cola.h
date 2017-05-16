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

  //#pragma pack(push, 1)
  struct Link {
    Time max_time;
    uint64_t chunk_id;
    uint64_t address;
  };
  //#pragma pack(pop)

  EXPORT Cola(const Param &p, Id measId, uint8_t *buffer);
  EXPORT Cola(uint8_t *buffer);
  /// Calculate needed buffer size for store index.
  EXPORT static size_t index_size(const Param &p);
  EXPORT uint8_t levels() const;
  EXPORT Id targetId() const;

  EXPORT bool addLink(uint64_t address, uint64_t chunk_id, Time maxTime);
  EXPORT std::list<Link> queryLink(Time from, Time to) const;

private:
  struct Private;
  std::shared_ptr<Private> _impl;
};
} // namespace storage
} // namespace dariadb