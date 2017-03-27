#include <libdariadb/flags.h>
#include <libdariadb/meas.h>
#include <libdariadb/utils/utils.h>
#include <algorithm>
#include <cmath>
#include <stdlib.h>
#include <string.h>

using namespace dariadb;

Meas::Meas() {
  id = Id();
  flag = Flag();
  value = Value();
  time = Time();
}

Meas::Meas(Id i) : Meas() {
  id = i;
}

Meas::Meas(const Meas &other) {
  id = other.id;
  time = other.time;
  value = other.value;
  flag = other.flag;
}

bool dariadb::areSame(Value a, Value b, const Value EPSILON) {
  return std::fabs(a - b) < EPSILON;
}

void dariadb::minmax_append(Id2MinMax_Ptr &out, const Id2MinMax_Ptr &source) {
  auto f = [&out](const Id2MinMax::value_type &v) {
    auto fres = out->insertion_pos(v.first);
    fres->v->second.updateMax(v.second.max);
    fres->v->second.updateMin(v.second.min);
  };
  source->apply(f);
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