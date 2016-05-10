#include "meas.h"
#include "utils/utils.h"
#include <cmath>
#include <stdlib.h>
#include <string.h>

using namespace dariadb;

Meas::Meas() {
  memset(this, 0, sizeof(Meas));
}

Meas Meas::empty() {
  return Meas{};
}

void Meas::readFrom(const Meas::PMeas m) {
  memcpy(this, m, sizeof(Meas));
}

bool dariadb::in_filter(Flag filter, Flag flg) {
  return (filter == 0) || (filter == flg);
}

bool dariadb::areSame(Value a, Value b, const Value EPSILON) {
  return std::fabs(a - b) < EPSILON;
}
