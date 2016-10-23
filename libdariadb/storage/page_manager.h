#pragma once

#include <libdariadb/interfaces/ichunkcontainer.h>
#include <libdariadb/interfaces/imeasstorage.h>
#include <libdariadb/utils/utils.h>
#include <libdariadb/storage/chunk.h>
#include <libdariadb/st_exports.h>
#include <libdariadb/storage/engine_environment.h>
#include <vector>

namespace dariadb {
namespace storage {

class PageManager : public utils::NonCopy, public IChunkContainer {
public:

public:
  EXPORT PageManager(const EngineEnvironment_ptr env);
  EXPORT virtual ~PageManager();
  EXPORT void flush();
  // ChunkContainer
  EXPORT bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                  dariadb::Time *maxResult) override;
  EXPORT ChunkLinkList chunksByIterval(const QueryInterval &query) override;
  EXPORT Id2Meas valuesBeforeTimePoint(const QueryTimePoint &q) override;
  EXPORT void readLinks(const QueryInterval &query, const ChunkLinkList &links,
                 IReaderClb *clb) override;

  EXPORT size_t files_count() const;
  EXPORT size_t chunks_in_cur_page() const;
  EXPORT dariadb::Time minTime();
  EXPORT dariadb::Time maxTime();

  EXPORT void append(const std::string &file_prefix, const dariadb::MeasArray &ma);

  EXPORT void fsck(bool force_check = true); // if false - check files openned for write-only

  EXPORT void eraseOld(const Time t);
  EXPORT void erase_page(const std::string &fname);
  EXPORT static void erase(const std::string& storage_path, const std::string &fname);
  EXPORT void compactTo(uint32_t pagesCount);
  EXPORT void compactbyTime(Time from, Time to);
private:
  class Private;
  std::unique_ptr<Private> impl;
};

typedef std::shared_ptr<PageManager> PageManager_ptr;
}
}
