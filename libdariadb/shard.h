#pragma once

#include <libdariadb/interfaces/iengine.h>
#include <libdariadb/st_exports.h>

namespace dariadb {
class ShardEngine;
using ShardEngine_Ptr = std::shared_ptr<ShardEngine>;
class ShardEngine : public IEngine {
public:
  struct Shard {
    std::string path;
    std::string alias;
    IdSet ids;
  };

  EXPORT static ShardEngine_Ptr create(const std::string &path);
  /**
   shard description with empty ids used as shard by default for values,
   that not exists in others shard.
   */
  EXPORT void shardAdd(const Shard &d);
  EXPORT std::list<Shard> shardList();

  EXPORT Status append(const Meas &value)override;
  EXPORT Time minTime() override;
  EXPORT Time maxTime() override;
  EXPORT Id2MinMax loadMinMax()override;
  EXPORT bool minMaxTime(Id id, Time *minResult, Time *maxResult) override;
  EXPORT void foreach (const QueryInterval &q, IReadCallback * clbk) override;
  EXPORT void foreach(const QueryTimePoint &q, IReadCallback * clbk) override;
  EXPORT Id2Cursor intervalReader(const QueryInterval &query) override;
  EXPORT Id2Meas readTimePoint(const QueryTimePoint &q) override;
  EXPORT Id2Meas currentValue(const IdArray &ids, const Flag &flag) override;
  EXPORT Statistic stat(const Id id, Time from, Time to) override;

  EXPORT void fsck() override;
  EXPORT void eraseOld(const Time &t) override;
  EXPORT void repack() override;
  EXPORT void stop() override;

  
  EXPORT Description description() const override;
  EXPORT void wait_all_asyncs() override;
  
  EXPORT void drop_part_wals(size_t count) override;
protected:
  EXPORT ShardEngine(const std::string &path);

private:
  class Private;
  std::unique_ptr<Private> _impl;

 
};
}