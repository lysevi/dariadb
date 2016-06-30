#pragma once

#include "../meas.h"
#include <algorithm>
#include <functional>

namespace dariadb {
namespace storage {

struct QueryParam {
  IdArray ids;
  Flag flag;
  Flag source;
  QueryParam(const IdArray &_ids, Flag _flag) : ids(_ids), flag(_flag), source(0) {}
  QueryParam(const IdArray &_ids, Flag _flag, Flag _source) : ids(_ids), flag(_flag), source(_source) {}
};

struct QueryInterval : public QueryParam {
  Time from;
  Time to;

  QueryInterval(const IdArray &_ids, Flag _flag, Time _from, Time _to)
      : QueryParam(_ids, _flag), from(_from), to(_to) {}

  QueryInterval(const IdArray &_ids, Flag _flag, Flag _source, Time _from, Time _to)
	  : QueryParam(_ids, _flag, _source), from(_from), to(_to) {}
};

struct QueryTimePoint : public QueryParam {
  Time time_point;

  QueryTimePoint(const IdArray &_ids, Flag _flag, Time _time_point)
      : QueryParam(_ids, _flag), time_point(_time_point) {}
};

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
}
}
