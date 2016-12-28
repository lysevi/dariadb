#include <libdariadb/flags.h>
#include <libdariadb/storage/bystep/bysteptrack.h>
#include <libdariadb/storage/bystep/helpers.h>
#include <cassert>
#include <cstring>

using namespace dariadb;
using namespace dariadb::storage;
using namespace dariadb::storage::bystep;

ByStepTrack::ByStepTrack(const Id target_id_, const STEP_KIND step_, uint64_t period_) {
  was_updated = false;
  must_be_replaced = false;
  _target_id = target_id_;
  _step = step_;
  _period = period_;

  auto values_size = bystep::step_to_size(_step);
  _values.resize(values_size);
  auto stepTime = stepByKind(step_);
  auto zero_time = get_zero_time(_period, _step);
  for (size_t i = 0; i < values_size; ++i) {
    _values[i].time = zero_time;
    _values[i].flag = Flags::_NO_DATA;
    _values[i].id = target_id_;
    zero_time += stepTime;
  }

  _minTime = MAX_TIME;
  _maxTime = MIN_TIME;
}

Time ByStepTrack::get_zero_time(const uint64_t p, const STEP_KIND s) {
  auto values_size = bystep::step_to_size(s);
  auto stepTime = stepByKind(s);
  auto zero_time = (p * values_size) * stepTime;
  return zero_time;
}

void ByStepTrack::from_chunk(const Chunk_Ptr &c) {
  auto rdr = c->getReader();
  while (!rdr->is_end()) {
    auto v = rdr->readNext();
    if (v.flag != Flags::_NO_DATA) {
      this->append(v);
    }
  }
}

Status ByStepTrack::append(const Meas &value) {
  auto pos = position_for_time(value.time);
  _values[pos] = value;
  _minTime = std::min(_minTime, value.time);
  _maxTime = std::max(_maxTime, value.time);
  was_updated = true;
  return Status(1, 0);
}

size_t ByStepTrack::position_for_time(const Time t) {
  auto rounded_tuple = bystep::roundTime(_step, t);
  auto r_time = std::get<0>(rounded_tuple);
  auto r_kind = std::get<1>(rounded_tuple);
  auto pos = ((r_time / r_kind)) % _values.size();
  assert(pos < _values.size());
  return pos;
}

Time ByStepTrack::minTime() {
  return _minTime;
}
Time ByStepTrack::maxTime() {
  return _maxTime;
}

Id2MinMax ByStepTrack::loadMinMax() {
  Id2MinMax result;
  MeasMinMax mm;
  bool min_found = false;
  for (size_t i = 0; i < _values.size(); ++i) {
    if (_values[i].flag != Flags::_NO_DATA) {
      if (!min_found) {
        mm.min = _values[i];
        min_found = true;
      }
      mm.max = _values[i];
    }
  }
  if (min_found) {
    result[_target_id] = mm;
  }
  return result;
}

Id2Meas ByStepTrack::currentValue(const IdArray &, const Flag &flag) {
  Id2Meas result{};
  for (size_t i = _values.size() - 1; ; --i) {
    if (_values[i].flag != Flags::_NO_DATA) {
      if (_values[i].inFlag(flag)) {
        result[_target_id] = _values[i];
      }
	  if (i == 0) {
		  break;
	  }
      break;
    }
  }
  return result;
}
bool ByStepTrack::minMaxTime(Id id, Time *minResult, Time *maxResult) {
  if (id != _target_id) {
    THROW_EXCEPTION("minMaxTime: logic error.");
    return false;
  }
  *minResult = MAX_TIME;
  *maxResult = MIN_TIME;
  bool result = false;
  for (size_t i = 0; i < _values.size(); ++i) {
    auto v = _values[i];
    if (v.flag != Flags::_NO_DATA) {
      result = true;
      *maxResult = std::max(*maxResult, v.time);
      *minResult = std::min(*minResult, v.time);
    }
  }
  return result;
}

void ByStepTrack::foreach (const QueryInterval &q, IReaderClb * clbk) {
  if (std::find(q.ids.begin(), q.ids.end(), _target_id) == q.ids.end()) {
    return;
  }
  for (size_t i = 0; i < _values.size(); ++i) {
    auto v = _values[i];
    if (v.time >= q.from && v.time < q.to) {
      if (!v.inFlag(q.flag)) {
        v.flag = Flags::_NO_DATA;
      }
      clbk->call(v);
    }
  }
}

Id2Meas ByStepTrack::readTimePoint(const QueryTimePoint &q) {
  Id2Meas result;
  result[_target_id] = _values[position_for_time(q.time_point)];
  return result;
}

uint64_t ByStepTrack::period() const {
  return _period;
}

size_t ByStepTrack::size() const {
  size_t result = 0;
  for (auto v : _values) {
    if (v.flag != Flags::_NO_DATA) {
      result++;
    }
  }
  return result;
}

Chunk_Ptr ByStepTrack::pack() const {
  size_t it = 0;

  auto buffer_size = _values.size() * sizeof(Meas);
  uint8_t *buffer = new uint8_t[buffer_size];
  ChunkHeader *chdr = new ChunkHeader;
  memset(buffer, 0, buffer_size);
  memset(chdr, 0, sizeof(ChunkHeader));

  Chunk_Ptr result{new ZippedChunk{chdr, buffer, buffer_size, _values[it]}};
  ++it;
  for (; it < _values.size(); ++it) {
    result->append(_values[it]);
  }
  result->close(); 
  result->header->id = _period;
  result->is_owner = true;
  return result;
}
