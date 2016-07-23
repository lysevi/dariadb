#include "imeassource.h"

#include <map>

using namespace dariadb;
using namespace dariadb::storage;

void IMeasSource::foreach (const QueryTimePoint &q, IReaderClb * clbk) {
  auto values = this->readInTimePoint(q);
  for (auto &kv : values) {
    clbk->call(kv.second);
  }
}
