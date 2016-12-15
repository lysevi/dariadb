#include <libdariadb/storage/bystep/bystep_storage.h>
#include <libdariadb/storage/bystep/io_adapter.h>
#include <libdariadb/storage/chunk.h>
#include <libdariadb/storage/settings.h>
#include <libdariadb/timeutil.h>
#include <libdariadb/utils/fs.h>
#include <libdariadb/flags.h>
#include <memory>
#include <tuple>
#include <vector>
#include <cassert>
#include <cstring>

using namespace dariadb;
using namespace dariadb::storage;

const char *filename = "bystep.db";

const size_t VALUES_PER_SEC = 600;
const size_t VALUES_PER_MIN = 60;
const size_t VALUES_PER_HR = 24;

namespace bystep_inner {
size_t step_to_size(StepKind kind) {
  switch (kind) {
  case StepKind::SECOND:
    return VALUES_PER_SEC;
  case StepKind::MINUTE:
    return VALUES_PER_MIN;
  case StepKind::HOUR:
    return VALUES_PER_HR;
  default:
	  return 0;
  }
}
static Time stepByKind(const StepKind stepkind) {
	Time step = 0;
	switch (stepkind) {
	case StepKind::SECOND:
		step = 1000;
		break;
	case StepKind::MINUTE:
		step = 60 * 1000;
		break;
	case StepKind::HOUR:
		step = 3600 * 1000;
		break;
	}
	return step;
}

/// result - rounded time and step in miliseconds
static std::tuple<Time, Time> roundTime(const StepKind stepkind, const Time t) {
  Time rounded = 0;
  Time step = 0;
  switch (stepkind) {
  case StepKind::SECOND:
    rounded = timeutil::round_to_seconds(t);
    step = 1000;
    break;
  case StepKind::MINUTE:
    rounded = timeutil::round_to_minutes(t);
    step = 60 * 1000;
    break;
  case StepKind::HOUR:
    rounded = timeutil::round_to_hours(t);
    step = 3600 * 1000;
    break;
  }
  return std::tie(rounded, step);
}

static uint64_t intervalForTime(const StepKind stepkind, const size_t valsInInterval,
                                const Time t) {
  Time rounded = 0;
  Time step = 0;
  auto rs = roundTime(stepkind, t);
  rounded = std::get<0>(rs);
  step = std::get<1>(rs);

  auto stepped = (rounded / step);
  if (stepped == valsInInterval) {
    return 1;
  }
  if (stepped < valsInInterval) {
    return 0;
  }
  return stepped / valsInInterval;
}
}

// TODO move to file
class ByStepTrack : public IMeasStorage {
public:
  ByStepTrack(const Id target_id_, const StepKind step_, uint64_t  period_) {
    _target_id;
    _step = step_;
	_period = period_;

	auto values_size = bystep_inner::step_to_size(_step);
	_values.resize(values_size);
	auto stepTime = bystep_inner::stepByKind(step_);
	auto zero_time = (period_*values_size)* stepTime;
	for (size_t i = 0; i < values_size; ++i) {
		_values[i].time = zero_time;
		_values[i].flag = Flags::_NO_DATA;
		_values[i].id = target_id_;
		zero_time += stepTime;
	}

	_minTime = MAX_TIME;
	_maxTime = MIN_TIME;
  }

  Status append(const Meas &value) override { 
	  auto pos = position_for_time(value.time);
	  _values[pos] = value;
	  _minTime = std::min(_minTime, value.time);
	  _maxTime = std::max(_maxTime, value.time);
	  return Status(1, 0); 
  }

  size_t position_for_time(const Time t) {
	  auto rounded_tuple = bystep_inner::roundTime(_step, t);
	  auto r_time = std::get<0>(rounded_tuple);
	  auto	r_kind = std::get<1>(rounded_tuple);
	  auto pos = ((r_time / r_kind) - (r_kind*_target_id)) % _values.size();
	  assert(pos < _values.size());
	  return pos;
  }

  Time minTime() override { //TODO refact
	  return _minTime;
  }
  Time maxTime() override { //TODO refact
	  return _maxTime;
  }

  bool minMaxTime(Id id, Time *minResult, Time *maxResult) override {
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

  void foreach (const QueryInterval &q, IReaderClb * clbk) override {
    for (size_t i = 0; i < _values.size(); ++i) {
      auto v = _values[i];
      if (v.inQuery(q.ids, q.flag) && (v.time>=q.from && v.time<q.to)) {
        clbk->call(v);
      }
    }
  }

  Id2Meas readTimePoint(const QueryTimePoint &q) override { 
	  Id2Meas result;
	  result[_target_id] = _values[position_for_time(q.time_point)];
	  return result;
  }

  Id2Meas currentValue(const IdArray &ids, const Flag &flag) override {
    return Id2Meas();
  }

  uint64_t period()const {
	  return _period;
  }

  size_t size()const {
	  size_t result = 0;
	  for (auto v: _values) {
		  if (v.flag!=Flags::_NO_DATA) {
			  result++;
		  }
	  }
	  return result;
  }

  Chunk_Ptr pack()const {
	  size_t it = 0;

	  auto buffer_size = _values.size() * sizeof(Meas);
	  uint8_t*buffer = new uint8_t[buffer_size];
	  ChunkHeader*chdr = new ChunkHeader;
	  memset(buffer, 0, buffer_size);
	  memset(chdr, 0, sizeof(ChunkHeader));

	  
	  Chunk_Ptr result{ new ZippedChunk{chdr, buffer,buffer_size, _values[it]} };
	  ++it;
	  for (; it < _values.size(); ++it) {
		  result->append(_values[it]);
	  }
	  result->close(); //TODO resize buffer like in page::create
	  result->header->id = _period;
	  result->is_owner = true;
	  return result;
  }
protected:
  Id _target_id;
  StepKind _step;
  std::vector<Meas> _values;
  uint64_t _period;
  Time _minTime;
  Time _maxTime;
};

using ByStepTrack_ptr = std::shared_ptr<ByStepTrack>;

struct ByStepStorage::Private : public IMeasStorage {
  Private(const EngineEnvironment_ptr &env)
      : _env(env), _settings(_env->getResourceObject<Settings>(
                       EngineEnvironment::Resource::SETTINGS)) {
    auto fname = utils::fs::append_path(_settings->path, filename);
    logger_info("engine: opening  bystep storage...");
    _io = std::make_unique<IOAdapter>(fname);
    logger_info("engine: bystep storage file opened.");
  }
  void stop() {
    if (_io != nullptr) {
      _io->stop();
      _io = nullptr;
    }
  }
  ~Private() { stop(); }

  void set_steps(const Id2Step &steps) { _steps = steps; }

  Status append(const Meas &value) override {
	  auto stepKind_it = _steps.find(value.id);
	  if (stepKind_it == _steps.end()) {
		  logger_fatal("engine: bystep - write values id:", value.id, " with unknow step.");
		  return Status(0, 1);
	  }

	  auto vals_per_interval = bystep_inner::step_to_size(stepKind_it->second);
	  auto period_num = bystep_inner::intervalForTime(stepKind_it->second, vals_per_interval, value.time);
	  
	  auto it = _values.find(value.id);
	  ByStepTrack_ptr ptr = nullptr;
	  if (it == _values.end()) {
		  ptr = ByStepTrack_ptr{ new ByStepTrack(value.id, stepKind_it->second, period_num) };
		  _values[value.id] = ptr;
	  }
	  else {
		  ptr = it->second;
	  }
	  
	  if (ptr->period() < period_num) {
		  auto old_value = ptr;
		  auto packed_chunk = old_value->pack();
		  if (packed_chunk != nullptr) {
			  //TODO write async.
			  _io->append(packed_chunk, old_value->minTime(), old_value->maxTime());
		  }
		  ptr = ByStepTrack_ptr{ new ByStepTrack(value.id, stepKind_it->second, period_num) };
		  _values[value.id] = ptr;
	  }
	  if (ptr->period() > period_num) {
		  NOT_IMPLEMENTED;
	  }
	  else {
		  auto res = ptr->append(value);
		  if (res.writed != 1) {
			  THROW_EXCEPTION("engine: bystep - logic_error.");
		  }
	  }
	  return Status(1, 0); 
  }

  Id2MinMax loadMinMax() override {
    Id2MinMax result;
    return result;
  }

  Time minTime() override {
	  Time result = _io->minTime();
	  for (auto&kv : _values) {
		  result = std::min(result, kv.second->minTime());
	  }
    return result;
  }

  Time maxTime() override {
	Time result = _io->maxTime();
	for (auto&kv : _values) {
		result = std::max(result, kv.second->maxTime());
	}
	return result;
  }

  bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                  dariadb::Time *maxResult) override {
	  auto res = _io->minMaxTime(id, minResult, maxResult);

	  auto fres = _values.find(id);
	  if (fres != _values.end()) {
		  Time subMin, subMax;
		  auto sub_res = fres->second->minMaxTime(id, &subMin, &subMax);
		  if (sub_res) {
			  res = true;
			  *minResult = std::min(*minResult, subMin);
			  *maxResult = std::max(*maxResult, subMax);
		  }
	  }
	  return res;
  }

  void foreach (const QueryInterval &q, IReaderClb * clbk) override {
    QueryInterval local_q = q;
    local_q.ids.resize(1);
    for (auto id : q.ids) {
	  auto step_kind_it = _steps.find(id);
	  if (step_kind_it == _steps.end()) {
		  logger_fatal("engine: bystep - unknow id:",id);
		  continue;
	  }
      
	  auto round_from = bystep_inner::roundTime(step_kind_it->second, q.from);
	  auto round_to = bystep_inner::roundTime(step_kind_it->second, q.to+std::get<1>(round_from));

	  auto vals_per_interval = bystep_inner::step_to_size(step_kind_it->second);

	  auto period_from = bystep_inner::intervalForTime(step_kind_it->second, vals_per_interval, std::get<0>(round_from));
	  auto period_to = bystep_inner::intervalForTime(step_kind_it->second, vals_per_interval, std::get<0>(round_to));

	  local_q.ids[0] = id;
	  local_q.from = std::get<0>(round_from);
	  local_q.to = std::get<0>(round_to);
      auto readed_chunks = _io->readInterval(period_from, period_to, id);

      for (auto c : readed_chunks) {
        foreach_chunk(q, clbk, c);
      }

      auto it = _values.find(id);
      if (it != _values.end()) {
        it->second->foreach (local_q, clbk);
      }
    }
  }

  void foreach_chunk(const QueryInterval &q, IReaderClb * clbk, const Chunk_Ptr&c) {
	  auto rdr = c->getReader();
	  while (!rdr->is_end()) {
		  auto v = rdr->readNext();
		  if (v.inQuery(q.ids, q.flag) && (v.time>=q.from && v.time<q.to)) {
			  clbk->call(v);
		  }
	  }
  }

  Id2Meas readTimePoint(const QueryTimePoint &q) override {
    Id2Meas result;

    auto local_q = q;
    local_q.ids.resize(1);
    // TODO refact.
    for (auto &id : q.ids) {

      auto stepKind_it = _steps.find(id);
      if (stepKind_it == _steps.end()) {
        logger_fatal("engine: bystep - readTimePoint id:", id, " with unknow step.");
        continue;
      }
      // find rounded time point for step
      auto round_tp = bystep_inner::roundTime(stepKind_it->second, q.time_point);

      local_q.ids[0] = id;
      local_q.time_point = std::get<0>(round_tp);

      result[id].time = local_q.time_point;
      result[id].flag = Flags::_NO_DATA;

      auto vals_per_interval = bystep_inner::step_to_size(stepKind_it->second);
      auto period_num = bystep_inner::intervalForTime(
          stepKind_it->second, vals_per_interval, local_q.time_point);

      auto fres = _values.find(id);
      if (fres != _values.end() && fres->second->period() == period_num) {
        auto local_res = fres->second->readTimePoint(local_q);
        result[id] = local_res[id];
      } else {
        auto chunk = _io->readTimePoint(period_num, id);
        if (chunk == nullptr) {
          result[id].time = local_q.time_point;
          result[id].flag = Flags::_NO_DATA;
        } else {
          auto rdr = chunk->getReader();
          while (!rdr->is_end()) {
            auto v = rdr->readNext();
            auto rounded_time_tuple =
                bystep_inner::roundTime(stepKind_it->second, v.time);
            auto rounded_time = std::get<0>(rounded_time_tuple);
            if (v.inQuery(local_q.ids, local_q.flag) &&
                rounded_time == local_q.time_point) {
              result[id] = v;
              break;
            }
            if (v.time > local_q.time_point) {
              break;
            }
          }
        }
      }
    }
    return result;
  }

  Id2Meas currentValue(const IdArray &ids, const Flag &flag) override {
    return Id2Meas();
  }

  void flush() override {}

  EngineEnvironment_ptr _env;
  storage::Settings *_settings;
  std::unique_ptr<IOAdapter> _io;
  std::map<Id, ByStepTrack_ptr> _values;
  Id2Step _steps;
};

ByStepStorage::ByStepStorage(const EngineEnvironment_ptr &env)
    : _impl(new ByStepStorage::Private(env)) {}

ByStepStorage::~ByStepStorage() {
  _impl = nullptr;
}

void ByStepStorage::set_steps(const Id2Step &steps) {
  _impl->set_steps(steps);
}

Time ByStepStorage::minTime() {
  return _impl->minTime();
}

Time ByStepStorage::maxTime() {
  return _impl->maxTime();
}

bool ByStepStorage::minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                               dariadb::Time *maxResult) {
  return _impl->minMaxTime(id, minResult, maxResult);
}

void ByStepStorage::foreach (const QueryInterval &q, IReaderClb * clbk) {
  _impl->foreach (q, clbk);
}

Id2Meas ByStepStorage::readTimePoint(const QueryTimePoint &q) {
  return _impl->readTimePoint(q);
}

Id2Meas ByStepStorage::currentValue(const IdArray &ids, const Flag &flag) {
  return _impl->currentValue(ids, flag);
}

Status ByStepStorage::append(const Meas &value) {
  return _impl->append(value);
}

void ByStepStorage::flush() {
  _impl->flush();
}

void ByStepStorage::stop() {
  _impl->stop();
}

Id2MinMax ByStepStorage::loadMinMax() {
  return _impl->loadMinMax();
}

uint64_t ByStepStorage::intervalForTime(const StepKind step, const size_t valsInInterval,
                                        const Time t) {
  return bystep_inner::intervalForTime(step, valsInInterval, t);
}