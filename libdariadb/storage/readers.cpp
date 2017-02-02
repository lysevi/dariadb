#include <algorithm>
#include <libdariadb/storage/readers.h>
#include <libdariadb/utils/utils.h>

using namespace dariadb;
using namespace dariadb::storage;

namespace readers_inner {
Time get_top_time(IReader *r) {
  if (r->is_end()) {
    return MAX_TIME;
  } else {
    return r->top().time;
  }
}
void fill_top_times(std::vector<Time> &top_times,
                    const std::vector<Reader_Ptr> &readers) {
  size_t pos = 0;
  for (auto r : readers) {
    top_times[pos++] = get_top_time(r.get());
  }
}

/// return: pair(index,ptr)
std::pair<size_t, IReader *>
get_reader_with_min_time(std::vector<Time> &top_times,
                         const std::vector<Reader_Ptr> &readers) {
  Time min_time = MAX_TIME;
  size_t min_time_index = 0;
  for (size_t i = 0; i < top_times.size(); ++i) {
    if (min_time > top_times[i]) {
      min_time = top_times[i];
      min_time_index = i;
    }
  }
  auto reader_it = readers[min_time_index];
  return std::make_pair(min_time_index, reader_it.get());
}
}

FullReader::FullReader(MeasArray &ma):_ma(ma) {
  _index = size_t(0);
  Time _minTime = MAX_TIME;
  Time _maxTime = MIN_TIME;
  for (auto &v : _ma) {
    _minTime = std::min(_minTime, v.time);
    _maxTime = std::max(_maxTime, v.time);
  }
  ENSURE(!_ma.empty());
  ENSURE(_minTime != MAX_TIME);
  ENSURE(_minTime <= _maxTime);
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

Time FullReader::minTime() { return _minTime; }

Time FullReader::maxTime() { return _maxTime; }

MergeSortReader::MergeSortReader(const std::list<Reader_Ptr> &readers)
    : _readers(readers.begin(), readers.end()) {
  _top_times.resize(_readers.size());
  _is_end_status.resize(_top_times.size());
  readers_inner::fill_top_times(_top_times, _readers);

  Time _minTime = MAX_TIME;
  Time _maxTime = MIN_TIME;
  for (auto &r : _readers) {
    _minTime = std::min(_minTime, r->minTime());
    _maxTime = std::max(_maxTime, r->maxTime());
  }
  ENSURE(!_readers.empty());
  ENSURE(_minTime != MAX_TIME);
  ENSURE(_minTime <= _maxTime);
}

Meas MergeSortReader::readNext() {
  auto index_and_reader =
      readers_inner::get_reader_with_min_time(_top_times, _readers);

  auto reader_ptr = index_and_reader.second;

  auto result = reader_ptr->readNext();
  _top_times[index_and_reader.first] = readers_inner::get_top_time(reader_ptr);
  _is_end_status[index_and_reader.first] = reader_ptr->is_end();

  // skip duplicates.
  for (size_t i = 0; i < _readers.size(); ++i) {
    if (!_is_end_status[i] && _top_times[i] == result.time) {
      auto r = _readers[i].get();
      while (!r->is_end() && r->top().time == result.time) {
        r->readNext();
      }
      if (r->is_end()) {
        _top_times[i] = MAX_TIME;
        _is_end_status[i] = true;
      }
    }
  }
  return result;
}

bool MergeSortReader::is_end() const {
  for (size_t i = 0; i < _is_end_status.size(); ++i) {
    if (!_is_end_status[i]) {
      return false;
    }
  }
  return true;
}

Meas MergeSortReader::top() {
  ENSURE(!is_end());
  auto r = readers_inner::get_reader_with_min_time(_top_times, _readers);
  return r.second->top();
}

Time MergeSortReader::minTime() { return _minTime; }

Time MergeSortReader::maxTime() { return _maxTime; }

Id2Reader ReaderFactory::colapseReaders(const Id2ReadersList &i2r) {
  Id2Reader result;
  for (auto kv : i2r) {
    std::list<Reader_Ptr> readers{kv.second.begin(), kv.second.end()};

    MergeSortReader *msr = new MergeSortReader(readers);
    Reader_Ptr rptr{msr};
    result[kv.first] = rptr;
  }
  return result;
}