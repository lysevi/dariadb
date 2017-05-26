#include <libdariadb/flags.h>
#include <libdariadb/storage/cursors.h>
#include <libdariadb/utils/utils.h>
#include <algorithm>
#include <map>
#include <set>

using namespace dariadb;
using namespace dariadb::storage;

namespace cursors_inner {

Time get_top_time(ICursor *r) {
  if (r->is_end()) {
    return MAX_TIME;
  } else {
    return r->top().time;
  }
}
void fill_top_times(std::vector<Time> &top_times,
                    const std::vector<Cursor_Ptr> &readers) {
  size_t pos = 0;
  for (auto &r : readers) {
    top_times[pos++] = get_top_time(r.get());
  }
}

/// return: pair(index,ptr)
std::pair<size_t, ICursor *>
get_cursor_with_min_time(std::vector<Time> &top_times,
                         const std::vector<Cursor_Ptr> &readers) {
  Time min_time = MAX_TIME;
  size_t min_time_index = 0;
  for (size_t i = 0; i < top_times.size(); ++i) {
    if ((top_times[i] != MAX_TIME) && (min_time > top_times[i])) {
      min_time = top_times[i];
      min_time_index = i;
    }
  }
  auto reader_it = readers[min_time_index];
  ENSURE(min_time != MAX_TIME);
  ENSURE(!reader_it->is_end());
  return std::make_pair(min_time_index, reader_it.get());
}

CursorsList unpack_merge_readers(CursorsList &&readers) {
  CursorsList tmp_readers_list;

  for (auto &&r : readers) {
    // TODO use type enum.
    auto msr = dynamic_cast<MergeSortCursor *>(r.get());
    if (msr == nullptr) {
      ENSURE(!r->is_end());
      tmp_readers_list.emplace_back(std::move(r));
    } else {
      for (auto &&sub_reader : msr->_readers) {
        ENSURE(!sub_reader->is_end());
        tmp_readers_list.emplace_back(std::move(sub_reader));
      }
      msr->_readers.clear();
    }
  }
  return tmp_readers_list;
}

CursorsList unpack_linear_readers(CursorsList &&readers) {
  CursorsList tmp_readers_list;

  for (auto &&r : readers) {
    auto lsr = dynamic_cast<LinearCursor *>(r.get());
    if (lsr == nullptr) {
      ENSURE(!r->is_end());
      tmp_readers_list.emplace_back(std::move(r));
    } else {
      for (auto &&sub_reader : lsr->_readers) {
        ENSURE(!sub_reader->is_end());
        tmp_readers_list.emplace_back(std::move(sub_reader));
      }
      lsr->_readers.clear();
    }
  }
  return tmp_readers_list;
}
}

FullCursor::FullCursor(MeasArray &ma) : _ma(ma) {
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

Meas FullCursor::readNext() {
  ENSURE(!is_end());

  auto result = _ma[_index++];
  // skip duplicates.
  while (!is_end()) {
    auto next = _ma[_index];
    if (next.time != result.time) {
      break;
    }
    _index++;
  }
  return result;
}

bool FullCursor::is_end() const {
  return _index >= _ma.size();
}

Meas FullCursor::top() {
  if (!is_end()) {
    return _ma[_index];
  }
  THROW_EXCEPTION("is_end()");
}

Time FullCursor::minTime() {
  return _minTime;
}

Time FullCursor::maxTime() {
  return _maxTime;
}

size_t FullCursor::count() const {
  return _ma.size();
}

MergeSortCursor::MergeSortCursor(CursorsList &&readers) {
  CursorsList tmp_readers_list = cursors_inner::unpack_merge_readers(std::move(readers));

  _readers.reserve(tmp_readers_list.size());
  _values_count = size_t(0);
  for (auto &&r : tmp_readers_list) {
    ENSURE(!r->is_end());
    _values_count += r->count();
    _readers.emplace_back(std::move(r));
  }
  tmp_readers_list.clear();

  _top_times.resize(_readers.size());
  _is_end_status.resize(_top_times.size());
  cursors_inner::fill_top_times(_top_times, _readers);
  std::fill_n(_is_end_status.begin(), _is_end_status.size(), false);

  _minTime = MAX_TIME;
  _maxTime = MIN_TIME;
  for (auto &r : _readers) {
    _minTime = std::min(_minTime, r->minTime());
    _maxTime = std::max(_maxTime, r->maxTime());
    ENSURE(!r->is_end());
  }
  ENSURE(!_readers.empty());
  ENSURE(_minTime != MAX_TIME);
  ENSURE(_minTime <= _maxTime);
}

Meas MergeSortCursor::readNext() {
  ENSURE(!is_end());
  auto index_and_reader = cursors_inner::get_cursor_with_min_time(_top_times, _readers);

  auto cursor = index_and_reader.second;
  ENSURE(!_is_end_status[index_and_reader.first]);
  ENSURE(!cursor->is_end());

  auto result = cursor->readNext();
  _top_times[index_and_reader.first] = cursors_inner::get_top_time(cursor);
  _is_end_status[index_and_reader.first] = cursor->is_end();

  // skip duplicates.
  for (size_t i = 0; i < _readers.size(); ++i) {
    ENSURE(_is_end_status[i] == _readers[i]->is_end());

    if (!_is_end_status[i] && _top_times[i] == result.time) {
      auto r = _readers[i].get();
      while (!r->is_end()) {
        if (r->top().time != result.time) {
          break;
        }
        r->readNext();
      }
      _top_times[i] = cursors_inner::get_top_time(r);
      _is_end_status[i] = r->is_end();
    }

    ENSURE(_is_end_status[i] == _readers[i]->is_end());
  }
  return result;
}

bool MergeSortCursor::is_end() const {
  for (size_t i = 0; i < _is_end_status.size(); ++i) {
    if (!_is_end_status[i]) {
      return false;
    }
  }
  return true;
}

Meas MergeSortCursor::top() {
  ENSURE(!is_end());
  auto r = cursors_inner::get_cursor_with_min_time(_top_times, _readers);
  return r.second->top();
}

Time MergeSortCursor::minTime() {
  return _minTime;
}

Time MergeSortCursor::maxTime() {
  return _maxTime;
}

size_t MergeSortCursor::count() const {
  return _values_count;
}

LinearCursor::LinearCursor(CursorsList &&readers) {
  auto sub_readers = cursors_inner::unpack_linear_readers(std::move(readers));
  ENSURE(sub_readers.size() >= readers.size());

  std::vector<Cursor_Ptr> rv;
  rv.reserve(sub_readers.size());
  for (auto &&sr : sub_readers) {
    rv.emplace_back(std::move(sr));
  }

  std::sort(rv.begin(), rv.end(),
            [](auto &l, auto &r) { return l->minTime() < r->minTime(); });

  for (auto &&sr : rv) {
    _readers.emplace_back(std::move(sr));
  }

  _values_count = size_t(0);
  _minTime = MAX_TIME;
  _maxTime = MIN_TIME;
  for (auto &r : _readers) {
    _minTime = std::min(_minTime, r->minTime());
    _maxTime = std::max(_maxTime, r->maxTime());
    _values_count += r->count();
  }
  ENSURE(!_readers.empty());
  ENSURE(!is_end());
  ENSURE(_minTime != MAX_TIME);
  ENSURE(_minTime <= _maxTime);
}

Meas LinearCursor::readNext() {
  ENSURE(!is_end());

  auto result = _readers.front()->readNext();
  if (_readers.front()->is_end()) {
    _readers.pop_front();
  }
  return result;
}

bool LinearCursor::is_end() const {
  return _readers.empty();
}

Meas LinearCursor::top() {
  ENSURE(!is_end());
  return _readers.front()->top();
}

Time LinearCursor::minTime() {
  return _minTime;
}

Time LinearCursor::maxTime() {
  return _maxTime;
}

size_t LinearCursor::count() const {
  return _values_count;
}

Cursor_Ptr CursorWrapperFactory::colapseCursors(CursorsList &&readers_list) {
  std::vector<Cursor_Ptr> readers_vector{readers_list.begin(), readers_list.end()};
  typedef std::set<size_t> positions_set;
  std::map<size_t, positions_set> overlapped;
  positions_set processed_pos;

  CursorsList result_readers;
  for (size_t i = 0; i < readers_vector.size(); ++i) {
    bool have_overlap = false;
    for (size_t j = 0; j < readers_vector.size(); ++j) {
      if (i != j) {
        bool is_not_processed = processed_pos.find(j) == processed_pos.end();
        if (!is_linear_readers(readers_vector[i], readers_vector[j]) &&
            is_not_processed) {
          processed_pos.insert(i);
          processed_pos.insert(j);

          // i - can be already in list, because i overlapped with other reader.
          bool already_in_list = false;
          for (auto &kv : overlapped) {
            if (kv.second.find(i) != kv.second.end()) {
              already_in_list = true;
              kv.second.insert(j);
              break;
            }
          }
          if (!already_in_list) {
            overlapped[i].insert(j);
          }
          have_overlap = true;
        }
      }
    }
    if (processed_pos.find(i) == processed_pos.end() && !have_overlap) {
      result_readers.emplace_back(readers_vector[i]);
    }
  }

  for (const auto &kv : overlapped) {
    CursorsList rs;
    rs.emplace_back(readers_vector[kv.first]);
    for (size_t pos : kv.second) {
      rs.emplace_back(readers_vector[pos]);
    }
    Cursor_Ptr rptr = std::make_shared<MergeSortCursor>(std::move(rs));
    result_readers.emplace_back(rptr);
  }

  Cursor_Ptr rptr = std::make_shared<LinearCursor>(std::move(result_readers));
  return rptr;
}

Id2Cursor CursorWrapperFactory::colapseCursors(Id2CursorsList &&i2r) {
  Id2Cursor result;
  for (auto &&kv : i2r) {
    if (kv.second.size() == 1) {
      result[kv.first] = std::move(kv.second.front());
    } else {
      result[kv.first] = colapseCursors(std::move(kv.second));
    }
  }
  return result;
}

bool CursorWrapperFactory::is_linear_readers(const Cursor_Ptr &r1, const Cursor_Ptr &r2) {
  bool is_overlap = utils::intervalsIntersection(r1->minTime(), r1->maxTime(),
                                                 r2->minTime(), r2->maxTime());
  return !is_overlap;
}
