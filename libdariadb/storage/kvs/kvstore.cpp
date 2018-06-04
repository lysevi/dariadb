#include "kvstore.h"
#include <libdariadb/storage/kvs/kvstore.h>

using namespace dariadb;
using namespace dariadb::storage;

const uint8_t current_version = 0;

void init_box_in_ptr(uint8_t *data, std::size_t size, KVBoxKind kind) {
  auto hdr = (BoxHeader *)(data);
  hdr->version = current_version;
  hdr->size = size;
  hdr->is_full = 0;
  hdr->kind = static_cast<uint8_t>(kind);
}

FreeListBox::FreeListBox(uint8_t *data, std::size_t size) : _data(data), _size(size) {}

FreeListBox::FreeListBox_ptr FreeListBox::make_box(uint8_t *data, std::size_t size) {
  init_box_in_ptr(data, size, KVBoxKind::FreeList);
  return std::make_shared<FreeListBox>(data, size);
}

FreeListBox::FreeListBox_ptr FreeListBox::read_box(uint8_t *data, std::size_t size) {
  return std::make_shared<FreeListBox>(data, size);
}

ChunkBox::ChunkBox(uint8_t *data, std::size_t size) : _data(data), _size(size) {}

ChunkBox::ChunkBox_ptr ChunkBox::make_box(uint8_t *data, std::size_t size) {
  init_box_in_ptr(data, size, KVBoxKind::ChunkBox);
  return std::make_shared<ChunkBox>(data, size);
}

ChunkBox::ChunkBox_ptr ChunkBox::read_box(uint8_t *data, std::size_t size) {
  return std::make_shared<ChunkBox>(data, size);
}
void ChunkBox::appendChunks(const std::vector<storage::Chunk *> &a) {}

bool ChunkBox::minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                          dariadb::Time *maxResult) {
  return false;
}

Id2Meas ChunkBox::valuesBeforeTimePoint(const QueryTimePoint &q) {
  return Id2Meas();
}

Id2Cursor ChunkBox::intervalReader(const QueryInterval &query) {
  return Id2Cursor();
}

Statistic ChunkBox::stat(const Id id, Time from, Time to) {
  return Statistic();
}
