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
  QueryParam(const IdArray &_ids, Flag _flag, Flag _source)
      : ids(_ids), flag(_flag), source(_source) {}
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
}
}
