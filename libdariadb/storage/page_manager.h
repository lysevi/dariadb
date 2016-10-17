#pragma once

#include <libdariadb/interfaces/ichunkcontainer.h>
#include <libdariadb/interfaces/imeasstorage.h>
#include <libdariadb/utils/utils.h>
#include <libdariadb/storage/chunk.h>
#include <libdariadb/dariadb_st_exports.h>
#include <vector>

namespace dariadb {
namespace storage {

class PageManager : public utils::NonCopy, public IChunkContainer {
public:
protected:
  virtual ~PageManager();

  PageManager();

public:
  DARIADB_ST_EXPORTS static void start();
  DARIADB_ST_EXPORTS static void stop();
  DARIADB_ST_EXPORTS void flush();
  DARIADB_ST_EXPORTS static PageManager *instance();

  // ChunkContainer
  DARIADB_ST_EXPORTS bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                  dariadb::Time *maxResult) override;
  DARIADB_ST_EXPORTS ChunkLinkList chunksByIterval(const QueryInterval &query) override;
  DARIADB_ST_EXPORTS Id2Meas valuesBeforeTimePoint(const QueryTimePoint &q) override;
  DARIADB_ST_EXPORTS void readLinks(const QueryInterval &query, const ChunkLinkList &links,
                 IReaderClb *clb) override;

  DARIADB_ST_EXPORTS size_t files_count() const;
  DARIADB_ST_EXPORTS size_t chunks_in_cur_page() const;
  DARIADB_ST_EXPORTS dariadb::Time minTime();
  DARIADB_ST_EXPORTS dariadb::Time maxTime();

  DARIADB_ST_EXPORTS void append(const std::string &file_prefix, const dariadb::MeasArray &ma);

  DARIADB_ST_EXPORTS void fsck(bool force_check = true); // if false - check files openned for write-only

  DARIADB_ST_EXPORTS void eraseOld(const Time t);
  DARIADB_ST_EXPORTS static void erase(const std::string &fname);

private:
  static PageManager *_instance;
  class Private;
  std::unique_ptr<Private> impl;
};
}
}
