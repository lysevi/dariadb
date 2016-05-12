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

Meas Meas::empty(Id id) {
	auto res = empty();
	res.id = id;
	return res;
}

void Meas::readFrom(const Meas::PMeas m) {
  memcpy(this, m, sizeof(Meas));
}

bool dariadb::areSame(Value a, Value b, const Value EPSILON) {
  return std::fabs(a - b) < EPSILON;
}
