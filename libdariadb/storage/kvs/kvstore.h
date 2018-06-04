#pragma once

#include <libdariadb/storage/chunkcontainer.h>

namespace dariadb {
namespace storage {

enum class KVBoxKind { FreeList, ChunkBox, MetaData };

struct BoxHeader {
  uint8_t version;
  uint8_t kind;
  uint32_t size;
  uint8_t is_full;
};

class FreeListBox {
public:
  using FreeListBox_ptr = std::shared_ptr<FreeListBox>;
  FreeListBox() = delete;
  FreeListBox(uint8_t *data, std::size_t size);
  static FreeListBox_ptr make_box(uint8_t *data, std::size_t size);
  static FreeListBox_ptr read_box(uint8_t *data, std::size_t size);

private:
  uint8_t *_data;
  std::size_t _size;
};

class ChunkBox : ChunkContainer {
public:
  using ChunkBox_ptr = std::shared_ptr<ChunkBox>;
  ChunkBox() = delete;
  ChunkBox(uint8_t *data, std::size_t size);
  static ChunkBox_ptr make_box(uint8_t *data, std::size_t size);
  static ChunkBox_ptr read_box(uint8_t *data, std::size_t size);
  // Inherited via ChunkContainer
  virtual void appendChunks(const std::vector<storage::Chunk *> &a) override;
  virtual bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                          dariadb::Time *maxResult) override;
  virtual Id2Meas valuesBeforeTimePoint(const QueryTimePoint &q) override;
  virtual Id2Cursor intervalReader(const QueryInterval &query) override;
  virtual Statistic stat(const Id id, Time from, Time to) override;

private:
  uint8_t *_data;
  std::size_t _size;
};
} // namespace storage
} // namespace dariadb