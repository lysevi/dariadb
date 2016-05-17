#pragma once

#include "../storage.h"
#include "chunk.h"
#include <functional>

namespace dariadb {
namespace storage {

struct IdArrayHasher {
  std::size_t operator()(const dariadb::IdArray &ids) const {
    size_t result = 0;
    size_t num = 0;
    std::hash<dariadb::Id> id_hasher{};
    for (auto id : ids) {
      result ^= (id_hasher(id) << num);
      num++;
    }
    return result;
  }
};

struct QueryIntervalHasher {
  IdArrayHasher id_hasher;
  std::hash<dariadb::Time> time_hasher;
  std::hash<dariadb::Flag> flag_hasher;
  std::size_t operator()(const dariadb::storage::QueryInterval &qi) const {
    size_t result = 0;
    result = id_hasher(qi.ids);
    result ^= time_hasher(qi.from) << 1;
    result ^= time_hasher(qi.to) << 2;
    result ^= flag_hasher(qi.flag) << 3;
    return result;
  }
};

struct QueryTimePointHasher {
  IdArrayHasher id_hasher;
  std::hash<dariadb::Time> time_hasher;
  std::hash<dariadb::Flag> flag_hasher;
  std::size_t operator()(const dariadb::storage::QueryTimePoint &qp) const {
    size_t result = 0;
    result = id_hasher(qp.ids);
    result ^= time_hasher(qp.time_point) << 1;
    result ^= flag_hasher(qp.flag) << 2;
    return result;
  }
};
class MemoryStorage : public MeasSource {
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
