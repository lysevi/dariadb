#include <algorithm>
#include <libdariadb/storage/readers.h>
#include <libdariadb/utils/utils.h>
#include <map>
#include <set>

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

ReadersList unpack_readers(const ReadersList &readers) {
  ReadersList tmp_readers_list;

  for (auto r : readers) {
    // TODO use type enum.
    auto msr = dynamic_cast<MergeSortReader *>(r.get());
    if (msr == nullptr) {
      tmp_readers_list.emplace_back(r);
    } else {
      for (auto sub_reader : msr->_readers) {
        tmp_readers_list.emplace_back(sub_reader);
      }
    }
  }
  return tmp_readers_list;
}
}

FullReader::FullReader(MeasArray &ma) : _ma(ma) {
  _index = size_t(0);

  _minTime = MAX_TIME;
  _maxTime = MIN_TIME;
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

MergeSortReader::MergeSortReader(const ReadersList &readers) {
  ReadersList tmp_readers_list = readers_inner::unpack_readers(readers);

  _readers.reserve(tmp_readers_list.size());
  for (auto r : tmp_readers_list) {
    _readers.emplace_back(r);
  }

  _top_times.resize(_readers.size());
  _is_end_status.resize(_top_times.size());
  readers_inner::fill_top_times(_top_times, _readers);

  _minTime = MAX_TIME;
  _maxTime = MIN_TIME;
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

LinearReader::LinearReader(const ReadersList &readers) {
  std::vector<Reader_Ptr> rv(readers.begin(), readers.end());
  std::sort(rv.begin(), rv.end(),
            [](auto l, auto r) { return l->minTime() < r->minTime(); });
  _readers = std::list<Reader_Ptr>(rv.begin(), rv.end());
  _minTime = MAX_TIME;
  _maxTime = MIN_TIME;
  for (auto &r : _readers) {
    _minTime = std::min(_minTime, r->minTime());
    _maxTime = std::max(_maxTime, r->maxTime());
  }
  ENSURE(!_readers.empty());
  ENSURE(_minTime != MAX_TIME);
  ENSURE(_minTime <= _maxTime);
}

Meas LinearReader::readNext() {
  ENSURE(!is_end());

  auto result = _readers.front()->readNext();
  if (_readers.front()->is_end()) {
    _readers.pop_front();
  }
  return result;
}

bool LinearReader::is_end() const { return _readers.empty(); }

Meas LinearReader::top() {
  ENSURE(!is_end());
  return _readers.front()->top();
}

Time LinearReader::minTime() { return _minTime; }

Time LinearReader::maxTime() { return _maxTime; }

Reader_Ptr ReaderWrapperFactory::colapseReaders(const ReadersList &readers_list) {
  std::vector<Reader_Ptr> readers_vector{readers_list.begin(),
                                         readers_list.end()};
  typedef std::set<size_t> positions_set;
  std::map<size_t, positions_set> overlapped;
  positions_set processed_pos;

  ReadersList result_readers;
  for (size_t i = 0; i < readers_vector.size(); ++i) {
    bool have_overlap = false;
    for (size_t j = 0; j < readers_vector.size(); ++j) {
      if (i != j) {
        bool is_not_processed = processed_pos.find(j) == processed_pos.end();
        if (!is_linear_readers(readers_vector[i], readers_vector[j]) &&
            is_not_processed) {
          processed_pos.insert(i);
          processed_pos.insert(j);
          overlapped[i].insert(j);
          have_overlap = true;
        }
      }
    }
    if (processed_pos.find(i) == processed_pos.end() && !have_overlap) {
      result_readers.emplace_back(readers_vector[i]);
    }
  }

  for (auto kv : overlapped) {
    ReadersList rs;
    rs.emplace_back(readers_vector[kv.first]);
    for (size_t pos : kv.second) {
      rs.emplace_back(readers_vector[pos]);
    }
    MergeSortReader *msr = new MergeSortReader(rs);
    Reader_Ptr rptr{msr};
    result_readers.emplace_back(rptr);
  }

  LinearReader *lsr = new LinearReader(result_readers);
  Reader_Ptr rptr{lsr};
  return rptr;
}

Id2Reader ReaderWrapperFactory::colapseReaders(const Id2ReadersList &i2r) {
  Id2Reader result;
  for (auto kv : i2r) {
    if (kv.second.size() == 1) {
      result[kv.first] = kv.second.front();
    } else {
      result[kv.first] = colapseReaders(kv.second);
    }
  }
  return result;
}

bool ReaderWrapperFactory::is_linear_readers(const Reader_Ptr &r1,
                                          const Reader_Ptr &r2) {
  bool is_overlap = utils::intervalsIntersection(r1->minTime(), r1->maxTime(),
                                                 r2->minTime(), r2->maxTime());
  return !is_overlap;
}