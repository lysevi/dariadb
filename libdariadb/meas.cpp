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

std::shared_ptr<SplitedById> dariadb::splitById(const MeasArray &ma) {
  dariadb::IdSet dropped;
  auto count = ma.size();
  std::vector<bool> visited(count);
  auto begin = ma.cbegin();
  auto end = ma.cend();
  size_t i = 0;
  SplitedById_Ptr result=std::make_shared<SplitedById>();
  MeasArray current_id_values;
  current_id_values.resize(ma.size());

  ENSURE(current_id_values.size() != 0);
  ENSURE(current_id_values.size() == ma.size());

  for (auto it = begin; it != end; ++it, ++i) {
    if (visited[i]) {
      continue;
    }
    if (dropped.find(it->id) != dropped.end()) {
      continue;
    }

    visited[i] = true;
    size_t current_id_values_pos = 0;
    current_id_values[current_id_values_pos++] = *it;
    size_t pos = 0;
    for (auto sub_it = begin; sub_it != end; ++sub_it, ++pos) {
      if (visited[pos]) {
        continue;
      }
      if ((sub_it->id == it->id)) {
        current_id_values[current_id_values_pos++] = *sub_it;
        visited[pos] = true;
      }
    }
    dropped.insert(it->id);
    result->insert(std::make_pair(
        it->id, MeasArray{current_id_values.begin(),
                          current_id_values.begin() + current_id_values_pos}));
    current_id_values_pos = 0;
  }
  return result;
}