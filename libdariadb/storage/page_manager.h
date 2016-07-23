#pragma once

#include "../interfaces/ichunkcontainer.h"
#include "../interfaces/imeasstorage.h"
#include "../utils/utils.h"
#include "chunk.h"

#include <vector>

namespace dariadb {
namespace storage {

const uint16_t OPENNED_PAGE_CACHE_SIZE = 10;

class PageManager : public utils::NonCopy, public IChunkContainer, public IMeasWriter {
public:
  struct Params {
    std::string path;
    uint32_t chunk_per_storage;
    uint32_t chunk_size;
    uint16_t openned_page_chache_size; /// max oppend pages in cache(readonly
                                       /// pages stored).
    Params(const std::string storage_path, size_t chunks_per_storage,
           size_t one_chunk_size) {
      path = storage_path;
      chunk_per_storage = uint32_t(chunks_per_storage);
      chunk_size = uint32_t(one_chunk_size);
      openned_page_chache_size = OPENNED_PAGE_CACHE_SIZE;
    }
  };

  struct GCResult {
    size_t page_removed;
    size_t chunks_merged;
    GCResult() {
      page_removed = 0;
      chunks_merged = 0;
    }
  };

protected:
  virtual ~PageManager();

  PageManager(const Params &param);

public:
  static void start(const Params &param);
  static void stop();
  void flush() override;
  static PageManager *instance();

  // ChunkContainer
  bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                  dariadb::Time *maxResult) override;
  ChunkLinkList chunksByIterval(const QueryInterval &query) override;
  Meas::Id2Meas valuesBeforeTimePoint(const QueryTimePoint &q) override;
  void readLinks(const QueryInterval &query, const ChunkLinkList &links,
                 IReaderClb *clb) override;

  size_t files_count() const;
  size_t chunks_in_cur_page() const;
  dariadb::Time minTime();
  dariadb::Time maxTime();

  append_result append(const Meas &value) override;
  void append(const std::string&file_prefix, const dariadb::Meas::MeasArray&ma);

  void fsck(bool force_check = true); // if false - check files openned for write-only


private:
  static PageManager *_instance;
  class Private;
  std::unique_ptr<Private> impl;
};
}
}
