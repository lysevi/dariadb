#pragma once

#include <list>
#include <memory>
#include <set>
#include <stdint.h>
#include <vector>
#include "utils/in_interval.h"

namespace dariadb {
typedef uint64_t Time;
typedef uint64_t Id;
typedef uint32_t Flag;
typedef double Value;
typedef std::vector<Id> IdArray;
typedef std::set<Id> IdSet;

bool areSame(Value a, Value b, const Value EPSILON = 1E-5);

struct Meas {
  typedef Meas *PMeas;
  typedef std::vector<Meas> MeasArray;
  typedef std::list<Meas> MeasList;

  static Meas empty();

  Meas();
  void readFrom(const Meas::PMeas m);
  bool operator==(const Meas &other) const {
    return id == other.id && time == other.time && flag == other.flag &&
           areSame(value, other.value);
  }

  bool inFlag(Flag f) const { return (f == 0) || (f == flag); }

  bool inIds(const IdArray &ids) const {
    return (ids.size() == 0) ||
           (std::find(ids.begin(), ids.end(), id) != ids.end());
  }

  bool inQuery(const IdArray &ids, const Flag f) const {
    return inFlag(f) && inIds(ids);
  }

  bool inInterval(Time from, Time to) const {
	  return utils::inInterval(from, to, time);
  }

  bool inQuery(const IdArray &ids, const Flag f, Time from, Time to) const {
	  return inQuery(ids,f) && inInterval(from,to);
  }

  Id id;
  Time time;
  Value value;
  Flag flag;
  Flag src;
};

}
