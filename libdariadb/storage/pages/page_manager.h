#pragma once

#include <libdariadb/interfaces/icompactioncontroller.h>
#include <libdariadb/interfaces/imeasstorage.h>
#include <libdariadb/st_exports.h>
#include <libdariadb/storage/chunk.h>
#include <libdariadb/storage/chunkcontainer.h>
#include <libdariadb/storage/engine_environment.h>
#include <libdariadb/storage/pages/page.h>
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
  EXPORT Statistic stat(const Id id, Time from, Time to) override;
  EXPORT size_t files_count() const;
  EXPORT size_t chunks_in_cur_page() const;
  EXPORT dariadb::Time minTime();
  EXPORT dariadb::Time maxTime();

  EXPORT void append_async(const std::string &file_prefix, const dariadb::SplitedById &ma,
                           on_create_complete_callback callback);
  EXPORT void appendChunks(const std::vector<Chunk *> &a) override;

  EXPORT void fsck();

  EXPORT void eraseOld(const dariadb::Id id, const Time t);
  EXPORT void erase_page(const std::string &fname);
  EXPORT static void erase(const std::string &storage_path, const std::string &fname);
  EXPORT void repack();
  EXPORT Id2MinMax_Ptr loadMinMax();

  EXPORT void compact(ICompactionController *logic);

  EXPORT std::list<std::string> pagesOlderThan(const dariadb::Id id, const Time t);

protected:
  EXPORT PageManager(const EngineEnvironment_ptr env);

private:
  class Private;
  std::unique_ptr<Private> impl;
};
}
}
