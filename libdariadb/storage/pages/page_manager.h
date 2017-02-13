#pragma once

#include <libdariadb/storage/chunkcontainer.h>
#include <libdariadb/interfaces/imeasstorage.h>
#include <libdariadb/st_exports.h>
#include <libdariadb/storage/chunk.h>
#include <libdariadb/storage/engine_environment.h>
#include <libdariadb/utils/utils.h>
#include <vector>

namespace dariadb {
namespace storage {

class PageManager;
typedef std::shared_ptr<PageManager> PageManager_ptr;
class PageManager : public utils::NonCopy, public ChunkContainer {
public:
  EXPORT static PageManager_ptr create(const EngineEnvironment_ptr env);
  EXPORT virtual ~PageManager();
  EXPORT void flush();
  // ChunkContainer
  EXPORT bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                         dariadb::Time *maxResult) override;
  EXPORT Id2Meas valuesBeforeTimePoint(const QueryTimePoint &q) override;
  EXPORT Id2Cursor intervalReader(const QueryInterval &query) override;
  EXPORT Statistic stat(const Id id, Time from, Time to)override;
  EXPORT size_t files_count() const;
  EXPORT size_t chunks_in_cur_page() const;
  EXPORT dariadb::Time minTime();
  EXPORT dariadb::Time maxTime();

  EXPORT void append(const std::string &file_prefix, const dariadb::MeasArray &ma);
  EXPORT void appendChunks(const std::vector<Chunk *> &a, size_t count) override;

  EXPORT void fsck();

  EXPORT void eraseOld(const Time t);
  EXPORT void erase_page(const std::string &fname);
  EXPORT static void erase(const std::string &storage_path, const std::string &fname);
  EXPORT void repack();
  EXPORT Id2MinMax loadMinMax();

protected:
  EXPORT PageManager(const EngineEnvironment_ptr env);
private:
  class Private;
  std::unique_ptr<Private> impl;
};
}
}
