#include "storage.h"
#include "flags.h"
#include "meas.h"
#include <map>

using namespace dariadb;
using namespace dariadb::storage;

void MeasSource::foreach(const QueryTimePoint&q, ReaderClb*clbk) {
	auto values = this->readInTimePoint(q);
	for (auto&kv : values) {
		clbk->call(kv.second);
	}
}

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
