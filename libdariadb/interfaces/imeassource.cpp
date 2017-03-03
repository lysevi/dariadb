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

MeasArray IMeasSource::readInterval(const QueryInterval &q) {
  size_t max_count = 0;
  auto r = this->intervalReader(q);
  for (auto &kv : r) {
    max_count += kv.second->count();
  }
  auto clbk = std::make_unique<MArray_ReaderClb>(max_count);

  for (auto id : q.ids) {
    auto fres = r.find(id);
    if (fres != r.end()) {
      fres->second->apply(clbk.get(), q);
    }
  }
  clbk->is_end();
  //TODO make readerclb to write to pointer to measarray
  return MeasArray(clbk->marray.begin(), clbk->marray.end());
}
