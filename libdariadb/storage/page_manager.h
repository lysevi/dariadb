#pragma once

#include "../storage.h"
#include "../utils/utils.h"
#include "chunk.h"
#include "chunk_container.h"
#include "cursor.h"
#include "mode.h"

#include <vector>

namespace dariadb {
namespace storage {

class PageManager : public utils::NonCopy,
                    public ChunkContainer,
                    public MeasWriter {
public:
  struct Params {
    std::string path;
    uint32_t chunk_per_storage;
    uint32_t chunk_size;
    Params(const std::string storage_path, size_t chunks_per_storage,
           size_t one_chunk_size) {
      path = storage_path;
      chunk_per_storage = uint32_t(chunks_per_storage);
      chunk_size = uint32_t(one_chunk_size);
    }
  };

protected:
  virtual ~PageManager();

  PageManager(const Params &param);

public:
  typedef uint32_t handle;
  static void start(const Params &param);
  static void stop();
  void flush()override;
  static PageManager *instance();

  // bool append(const Chunk_Ptr &c) override;
  // bool append(const ChunksList &lst) override;

  // ChunkContainer
  bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                  dariadb::Time *maxResult) override;
  Cursor_ptr chunksByIterval(const QueryInterval &query) override;
  IdToChunkMap chunksBeforeTimePoint(const QueryTimePoint &q) override;
  IdArray getIds() override;

  // dariadb::storage::ChunksList get_open_chunks();
  size_t chunks_in_cur_page() const;
  size_t in_queue_size() const; // TODO rename to queue_size

  dariadb::Time minTime();
  dariadb::Time maxTime();

  append_result append(const Meas &value) override;

private:
  static PageManager *_instance;
  class Private;
  std::unique_ptr<Private> impl;
};
}
}
