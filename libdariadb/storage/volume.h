#pragma once

#include <libdariadb/meas.h>
#include <libdariadb/st_exports.h>

namespace dariadb {
namespace storage {

class VolumeIndex {
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
    bool erased;

    static Link makeEmpty() {
      return Link{MIN_TIME, std::numeric_limits<uint64_t>::max(),
                  std::numeric_limits<uint64_t>::max(), true};
    }

    bool IsEmpty() const {
      return erased ||
             (max_time == MIN_TIME && chunk_id == std::numeric_limits<uint64_t>::max() &&
              address == std::numeric_limits<uint64_t>::max());
    }

    bool operator==(const Link &other) {
      return erased == other.erased && max_time == other.max_time &&
             chunk_id == other.chunk_id && address == other.address;
    }
  };
  //#pragma pack(pop)

  EXPORT VolumeIndex(const Param &p, Id measId, uint8_t *buffer);
  EXPORT VolumeIndex(uint8_t *buffer);
  /// Calculate needed buffer size for store index.
  EXPORT static size_t index_size(const Param &p);
  EXPORT uint8_t levels() const;
  EXPORT Id targetId() const;

  EXPORT bool addLink(uint64_t address, uint64_t chunk_id, Time maxTime);
  EXPORT std::vector<Link> queryLink(Time from, Time to) const;
  EXPORT Link queryLink(Time tp) const;
  EXPORT void rm(Time maxTime, uint64_t chunk_id);

private:
  struct Private;
  std::shared_ptr<Private> _impl;
};
} // namespace storage
} // namespace dariadb