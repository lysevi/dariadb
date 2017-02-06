#include <algorithm>
#include <cmath>
#include <libdariadb/flags.h>
#include <libdariadb/meas.h>
#include <libdariadb/utils/utils.h>
#include <stdlib.h>
#include <string.h>

using namespace dariadb;

Meas::Meas() {
  id = Id();
  flag = Flag();
  value = Value();
  time = Time();
}

Meas::Meas(Id i) : Meas() { id = i; }

Meas::Meas(const Meas &other) {
  id = other.id;
  time = other.time;
  value = other.value;
  flag = other.flag;
}
bool dariadb::areSame(Value a, Value b, const Value EPSILON) {
  return std::fabs(a - b) < EPSILON;
}

void dariadb::minmax_append(Id2MinMax &out, const Id2MinMax &source) {
  for (auto kv : source) {
    auto fres = out.find(kv.first);
    if (fres == out.end()) {
      out[kv.first] = kv.second;
    } else {
      out[kv.first].updateMax(kv.second.max);
      out[kv.first].updateMin(kv.second.min);
    }
  }
}

void dariadb::mlist2mset(MeasList &mlist, Id2MSet &sub_result) {
  for (auto m : mlist) {
    if (m.flag == FLAGS::_NO_DATA) {
      continue;
    }
    sub_result[m.id].emplace(m);
  }
}

void MeasMinMax::updateMax(const Meas &m) {
  if (m.time > this->max.time) {
    this->max = m;
  }
}

void MeasMinMax::updateMin(const Meas &m) {
  if (m.time < this->min.time) {
    this->min = m;
  }
}