#include <libdariadb/meas.h>
#include <libdariadb/flags.h>
#include <libdariadb/utils/utils.h>
#include <cmath>
#include <stdlib.h>
#include <string.h>
#include <algorithm>

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

void dariadb::minmax_append(Id2MinMax&out, const Id2MinMax &source){
    for(auto kv:source){
        auto fres=out.find(kv.first);
        if(fres==out.end()){
            out[kv.first]=kv.second;
        }else{
			out[kv.first].updateMax(kv.second.max);
			out[kv.first].updateMin(kv.second.min);
        }
    }
}

void  dariadb::mlist2mset(MeasList &mlist, Id2MSet &sub_result) {
  for (auto m : mlist) {
    if (m.flag == Flags::_NO_DATA) {
      continue;
    }
    sub_result[m.id].emplace(m);
  }
}

void MeasMinMax::updateMax(const Meas&m) {
	if (m.time > this->max.time) {
		this->max = m;
	}
}

void MeasMinMax::updateMin(const Meas&m) {
	if (m.time < this->min.time) {
		this->min = m;
	}
}