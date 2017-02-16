#pragma once

#include <libdariadb/engine.h>
#include <libdariadb/st_exports.h>

namespace dariadb {
class ShardEngine;
using ShardEngine_Ptr = std::shared_ptr<ShardEngine>;
class ShardEngine : public IMeasStorage, public IEngine {
public:
  struct Description {
    const std::string path;
    const std::string name;
    const IdSet ids;
  };

  EXPORT static ShardEngine_Ptr create(const std::string &path);
  EXPORT void addShard(const Description &d);
  EXPORT Time minTime() override;
  EXPORT Time maxTime() override;
  EXPORT bool minMaxTime(Id id, Time *minResult, Time *maxResult) override;
  EXPORT void foreach (const QueryInterval &q, IReadCallback * clbk) override;
  EXPORT Id2Cursor intervalReader(const QueryInterval &query) override;
  EXPORT Id2Meas readTimePoint(const QueryTimePoint &q) override;
  EXPORT Id2Meas currentValue(const IdArray &ids, const Flag &flag) override;
  EXPORT Statistic stat(const Id id, Time from, Time to) override;

  EXPORT void fsck() override;
  EXPORT void eraseOld(const Time &t) override;
  EXPORT void repack() override;

protected:
  EXPORT ShardEngine(const std::string &path);

private:
  class Private;
  std::unique_ptr<Private> _impl;
};
}