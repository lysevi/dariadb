#include "imeaswriter.h"

using namespace dariadb;
using namespace dariadb::storage;

append_result IMeasWriter::append(const Meas::MeasArray::const_iterator &begin,
                                  const Meas::MeasArray::const_iterator &end) {
  dariadb::append_result ar{};

  for (auto it = begin; it != end; ++it) {
    ar = ar + this->append(*it);
  }
  return ar;
}

append_result IMeasWriter::append(const Meas::MeasList::const_iterator &begin,
                                  const Meas::MeasList::const_iterator &end) {
  dariadb::append_result ar{};
  for (auto it = begin; it != end; ++it) {
    ar = ar + this->append(*it);
  }
  return ar;
}
