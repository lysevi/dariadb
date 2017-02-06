#include <libdariadb/interfaces/imeassource.h>

#include <libdariadb/storage/callbacks.h>
#include <libdariadb/utils/utils.h>
#include <map>

using namespace dariadb;
using namespace dariadb::storage;

void IMeasSource::foreach (const QueryTimePoint &q, IReadCallback * clbk) {
  auto values = this->readTimePoint(q);
  for (auto &kv : values) {
    clbk->apply(kv.second);
  }
}

MeasList IMeasSource::readInterval(const QueryInterval &q) {
  auto clbk = std::make_unique<MList_ReaderClb>();
  this->foreach (q, clbk.get());
  Id2MSet sub_result;
  for (auto v : clbk->mlist) {
    sub_result[v.id].emplace(v);
  }
  MeasList result;
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

Id2MinMax IMeasSource::loadMinMax() {
  NOT_IMPLEMENTED;
  // return Id2MinMax();
}
