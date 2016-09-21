#pragma once

#include "../interfaces/ichunkcontainer.h"
#include "../interfaces/imeasstorage.h"
#include "../utils/utils.h"
#include "chunk.h"

#include <vector>

namespace dariadb {
namespace storage {

class PageManager : public utils::NonCopy, public IChunkContainer {
public:
protected:
  virtual ~PageManager();

  PageManager();

public:
  static void start();
  static void stop();
  void flush();
  static PageManager *instance();

  // ChunkContainer
  bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                  dariadb::Time *maxResult) override;
  ChunkLinkList chunksByIterval(const QueryInterval &query) override;
  Id2Meas valuesBeforeTimePoint(const QueryTimePoint &q) override;
  void readLinks(const QueryInterval &query, const ChunkLinkList &links,
                 IReaderClb *clb) override;

  size_t files_count() const;
  size_t chunks_in_cur_page() const;
  dariadb::Time minTime();
  dariadb::Time maxTime();

  void append(const std::string &file_prefix, const dariadb::MeasArray &ma);

  void fsck(bool force_check = true); // if false - check files openned for write-only

  void eraseOld(const Time t);
  static void erase(const std::string &fname);

private:
  static PageManager *_instance;
  class Private;
  std::unique_ptr<Private> impl;
};
}
}
