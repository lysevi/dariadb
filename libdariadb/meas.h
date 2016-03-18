#pragma once

#include <memory>
#include <vector>
#include <list>
#include <set>

namespace dariadb {
typedef uint64_t Time;
typedef uint64_t Id;
typedef uint32_t Flag;
typedef double Value;
typedef std::vector<Id> IdArray;
typedef std::set<Id> IdSet;

struct Meas {
  typedef Meas *PMeas;
  typedef std::vector<Meas> MeasArray;
  typedef std::list<Meas> MeasList;

  static Meas empty();

  Meas();
  Meas(const Meas&other)=default;
  void readFrom(const Meas::PMeas m);
  bool operator==(const Meas &other) const {
    return id == other.id && time == other.time && flag == other.flag &&
           value == other.value;
  }

  Id id;
  Time time;
  Value value;
  Flag flag;
};

bool in_filter(Flag filter, Flag flg);
}
