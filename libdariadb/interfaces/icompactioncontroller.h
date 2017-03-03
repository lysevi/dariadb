#pragma once

#include <libdariadb/meas.h>

namespace dariadb {

class ICompactionController {
public:
  ICompactionController(Time _from, Time _to) {
    from = _from;
    to = _to;
  }
  /**
  method to control compaction process.
  values - input vector of values. you can replace needed items
  filter - if filter[i]==0 values be ignored.
  */
  virtual void compact(dariadb::MeasArray &values, std::vector<int> &filter) = 0;

  Time from;
  Time to;
};
}
