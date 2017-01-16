#pragma once

#include <libdariadb/interfaces/imeasstorage.h>
#include <libdariadb/storage/bystep/step_kind.h>
#include <libdariadb/storage/chunk.h>

namespace dariadb {
namespace storage {
namespace bystep {
class ByStepTrack : public IMeasStorage {
public:
  ByStepTrack(const Id target_id_, const STEP_KIND step_, uint64_t period_);
  static Time get_zero_time(const uint64_t p, const STEP_KIND s);
  void from_chunk(const Chunk_Ptr &c);
  Status append(const Meas &value) override;
  size_t position_for_time(const Time t);
  Time minTime() override;
  Time maxTime() override;
  Id2MinMax loadMinMax() override;
  Id2Meas currentValue(const IdArray &, const Flag &flag) override;
  bool minMaxTime(Id id, Time *minResult, Time *maxResult) override;
  void foreach (const QueryInterval &q, IReaderClb * clbk) override;
  Id2Meas readTimePoint(const QueryTimePoint &q) override;
  uint64_t period() const;
  size_t size() const;

  Chunk_Ptr pack() const;

  bool must_be_replaced; /// true-if need replace chunk in disk storage.
  bool was_updated;      /// true - if was updated by 'append' method
protected:
  Id _target_id;
  STEP_KIND _step;
  std::vector<Meas> _values;
  uint64_t _period;
  Time _minTime;
  Time _maxTime;
};

using ByStepTrack_ptr = std::shared_ptr<ByStepTrack>;
}
}
}