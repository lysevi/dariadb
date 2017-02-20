#pragma once

#include <algorithm>
#include <functional>
#include <libdariadb/meas.h>

namespace dariadb {

struct QueryParam {
  IdArray ids;
  Flag flag;
  QueryParam(const IdArray &_ids, Flag _flag) : ids(_ids), flag(_flag) {}
};

struct QueryInterval : public QueryParam {
  Time from;
  Time to;
  Time step; /// ignored in disk operations
  QueryInterval(const IdArray &_ids, Flag _flag, Time _from, Time _to)
      : QueryParam(_ids, _flag), from(_from), to(_to), step(0) {}

  QueryInterval(const IdArray &_ids, Flag _flag, Time _from, Time _to,
                Time _step)
      : QueryParam(_ids, _flag), from(_from), to(_to), step(_step) {}
};

struct QueryTimePoint : public QueryParam {
  Time time_point;

  QueryTimePoint(const IdArray &_ids, Flag _flag, Time _time_point)
      : QueryParam(_ids, _flag), time_point(_time_point) {}
};
}
