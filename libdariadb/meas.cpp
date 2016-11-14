#include <libdariadb/meas.h>
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
            out[kv.first].min=std::min(kv.second.min,fres->second.min);
            out[kv.first].max=std::max(kv.second.max,fres->second.max);
        }
    }
}
