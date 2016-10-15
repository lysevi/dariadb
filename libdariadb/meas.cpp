#include <libdariadb/meas.h>
#include <libdariadb/utils/utils.h>
#include <cmath>
#include <stdlib.h>
#include <string.h>
#include <cassert>

using namespace dariadb;

Meas::Meas() {
  memset(this, 0, sizeof(Meas));
}

Meas Meas::empty() {
  return Meas{};
}

Meas Meas::empty(Id id) {
  auto res = empty();
  res.id = id;
  return res;
}

bool dariadb::areSame(Value a, Value b, const Value EPSILON) {
  return std::fabs(a - b) < EPSILON;
}

std::map<Id,MeasArray> dariadb::splitById(const MeasArray &ma) {
    dariadb::IdSet dropped;
    auto count = ma.size();
    std::vector<bool> visited(count);
    auto begin = ma.cbegin();
    auto end = ma.cend();
    size_t i = 0;
    std::map<Id, MeasArray> result;
    MeasArray current_id_values;
    current_id_values.resize(ma.size());

    assert(current_id_values.size() != 0);
    assert(current_id_values.size() == ma.size());

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
        result.insert(std::make_pair(it->id,
        MeasArray{ current_id_values.begin(), current_id_values.begin() + current_id_values_pos }));
        current_id_values_pos = 0;
    }
    return result;
}
