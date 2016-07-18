#include "storage.h"
#include "flags.h"
#include "meas.h"
#include <map>

using namespace dariadb;
using namespace dariadb::storage;

void MeasSource::foreach (const QueryTimePoint &q, ReaderClb * clbk) {
  auto values = this->readInTimePoint(q);
  for (auto &kv : values) {
    clbk->call(kv.second);
  }
}

append_result MeasWriter::append(const Meas::MeasArray::const_iterator &begin,
                                 const Meas::MeasArray::const_iterator &end) {
  dariadb::append_result ar{};

  for (auto it = begin; it != end; ++it) {
    ar = ar + this->append(*it);
  }
  return ar;
}

append_result MeasWriter::append(const Meas::MeasList::const_iterator &begin,
                                 const Meas::MeasList::const_iterator &end) {
  dariadb::append_result ar{};
  for (auto it = begin; it != end; ++it) {
    ar = ar + this->append(*it);
  }
  return ar;
}
