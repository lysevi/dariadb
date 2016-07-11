#include "storage.h"
#include "flags.h"
#include "meas.h"
#include <map>

using namespace dariadb;
using namespace dariadb::storage;


append_result MeasWriter::append(const Meas::MeasArray &ma) {
  dariadb::append_result ar{};
  for (auto &m : ma) {
    ar = ar + this->append(m);
  }
  return ar;
}

append_result MeasWriter::append(const Meas::MeasList &ml) {
  dariadb::append_result ar{};
  for (auto &m : ml) {
    ar = ar + this->append(m);
  }
  return ar;
}
