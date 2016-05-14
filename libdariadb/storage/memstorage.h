#pragma once

#include "../storage.h"
#include "chunk.h"

namespace dariadb {
namespace storage {

class MemoryStorage : public MeasSource{
public:
  MemoryStorage();
  virtual ~MemoryStorage();

//  Reader_ptr readInterval(Time from, Time to) override;
//  Reader_ptr readInTimePoint(Time time_point) override;
  Reader_ptr readInterval(const QueryInterval &q) override;
  Reader_ptr readInTimePoint(const QueryTimePoint &q) override;

  Reader_ptr currentValue(const IdArray &ids, const Flag &flag) override;
  
  Time minTime() override;
  Time maxTime() override;

  void set_chunkSource(ChunkContainer *cw);

protected:
  class Private;
  std::unique_ptr<Private> _Impl;
};
}
}
