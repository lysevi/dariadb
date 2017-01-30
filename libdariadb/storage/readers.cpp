#include <algorithm>
#include <libdariadb/storage/readers.h>
#include <libdariadb/utils/utils.h>

using namespace dariadb;
using namespace dariadb::storage;

namespace readers_inner {
Reader_Ptr reader_with_min_time(const std::list<Reader_Ptr> &readers) {
  std::vector<Time> top_times(readers.size());
  size_t pos = 0;
  for (auto r : readers) {
    if (r->is_end()) {
      top_times[pos++] = MAX_TIME;
    } else {
      top_times[pos++] = r->top().time;
    }
  }

  auto min_time = std::min_element(top_times.begin(), top_times.end(),
                                   [](auto t1, auto t2) { return t1 < t2; });
  auto min_time_index = std::distance(top_times.begin(), min_time);

  auto reader_it = readers.begin();
  std::advance(reader_it, min_time_index);
  return *reader_it;
}
}

FullReader::FullReader(MeasArray &ml) {
  _ma = ml;
  _index = size_t(0);
}

Meas FullReader::readNext() {
  ENSURE(!is_end());

  auto result = _ma[_index++];
  return result;
}

bool FullReader::is_end() const { return _index >= _ma.size(); }

Meas FullReader::top() {
  if (!is_end()) {
    return _ma[_index];
  }
  THROW_EXCEPTION("is_end()");
}

MergeSortReader::MergeSortReader(const std::list<Reader_Ptr> &readers)
    : _readers(readers) {}

Meas MergeSortReader::readNext() {
  auto reader_it = readers_inner::reader_with_min_time(_readers);

  auto result = reader_it->readNext();
  return result;
}

bool MergeSortReader::is_end() const {
  return !std::any_of(_readers.begin(), _readers.end(),
                      [](const auto &r) { return !r->is_end(); });
}

Meas MergeSortReader::top() {
  ENSURE(!is_end());
  auto r = readers_inner::reader_with_min_time(_readers);
  return r->top();
}

Id2Reader MergeSortReader::colapseReaders(const Id2ReadersList &i2r) {
  Id2Reader result;
  for (auto kv : i2r) {
    std::list<Reader_Ptr> readers{kv.second.begin(), kv.second.end()};

    MergeSortReader *msr = new MergeSortReader(readers);
    Reader_Ptr rptr{msr};
    result[kv.first] = rptr;
  }
  return result;
}