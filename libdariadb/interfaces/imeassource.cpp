#include "imeassource.h"

#include "../storage/callbacks.h"
#include "../utils/metrics.h"
#include <map>

using namespace dariadb;
using namespace dariadb::storage;

void IMeasSource::foreach (const QueryTimePoint &q, IReaderClb * clbk) {
  auto values = this->readTimePoint(q);
  for (auto &kv : values) {
    clbk->call(kv.second);
  }
}

Meas::MeasList IMeasSource::readInterval(const QueryInterval &q) {
  TIMECODE_METRICS(ctmd, "readInterval", "IMeasSource::readInterval");
  std::unique_ptr<MList_ReaderClb> clbk{new MList_ReaderClb};
  this->foreach (q, clbk.get());

  Id2MSet sub_result;
  for (auto v : clbk->mlist) {
    sub_result[v.id].insert(v);
  }
  Meas::MeasList result;
  for (auto id : q.ids) {
    auto sublist = sub_result.find(id);
    if (sublist == sub_result.end()) {
      continue;
    }
    for (auto v : sublist->second) {
      result.push_back(v);
    }
  }
  return result;
}