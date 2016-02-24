#include "meas.h"
#include "utils.h"
#include <stdlib.h>
#include <string.h>

using namespace timedb;

Meas::Meas() { 
    memset(this, 0, sizeof(Meas));
}

Meas Meas::empty() {
  return Meas{};
}

void Meas::readFrom(const Meas::PMeas m) {
    memcpy(this, m, sizeof(Meas));
}
