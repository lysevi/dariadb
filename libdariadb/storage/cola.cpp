#include <libdariadb/storage/cola.h>

using namespace dariadb;
using namespace dariadb::storage;

struct Cola::Private {

#pragma pack(push, 1)
  struct VolumeHeader {};

  struct Link {
    Time max_time;
    uint64_t address;
  };

  struct LevelHeader {};
#pragma pack(pop)

  Private(const Cola::Param &p, const uint8_t *buffer) {}

  static size_t one_block_size(size_t B) { return sizeof(Link) * B; }

  static size_t block_in_level(size_t lev_num) { return (size_t(1) << lev_num); }

  static size_t bytes_in_level(size_t B, size_t lvl) {
    auto blocks_count = block_in_level(lvl);
    auto res = one_block_size(B) * blocks_count;
    return res;
  }

  static size_t index_size(const Param &p) {
    size_t result = 0;
    result += one_block_size(p.B); /// space to _memvalues
    for (size_t lvl = 0; lvl < p.levels; ++lvl) {
      auto cur_level_links = bytes_in_level(p.B, lvl);
      result += sizeof(LevelHeader) + cur_level_links;
    }
    return result;
  }
};

Cola::Cola(const Param &p, const uint8_t *buffer) : _impl(new Cola::Private(p, buffer)) {}

size_t Cola::index_size(const Param &p) {
  return Cola::Private::index_size(p);
}