#include <libdariadb/storage/bystep/bystep_storage.h>
#include <libdariadb/storage/bystep/io_adapter.h>
#include <libdariadb/storage/chunk.h>
#include <libdariadb/storage/settings.h>
#include <libdariadb/timeutil.h>
#include <libdariadb/utils/fs.h>
#include <memory>
#include <tuple>
#include <vector>
#include <cassert>

using namespace dariadb;
using namespace dariadb::storage;

const char *filename = "bystep.db";

const size_t VALUES_PER_SEC = 60;
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
	_values.resize(bystep_inner::step_to_size(_step));
  }

  Status append(const Meas &value) override { 
	  auto rounded_tuple = bystep_inner::roundTime(_step, value.time);
	  auto r_time = std::get<0>(rounded_tuple);
	  auto	r_kind = std::get<1>(rounded_tuple);
	  auto pos = ((r_time/ r_kind) - (r_kind*_target_id)) %_values.size();
	  assert(pos < _values.size());
	  _values[pos] = value;
	  return Status(1, 0); 
  }

  Time minTime() override { return Time(); }
  Time maxTime() override { return Time(); }
  bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                  dariadb::Time *maxResult) override {
    return false;
  }
  void foreach (const QueryInterval &q, IReaderClb * clbk) override {}
  Id2Meas readTimePoint(const QueryTimePoint &q) override { return Id2Meas(); }
  Id2Meas currentValue(const IdArray &ids, const Flag &flag) override {
    return Id2Meas();
  }

  uint64_t period()const {
	  return _period;
  }

  size_t size()const {
	  size_t result = 0;
	  for (auto&v : _values) {
		  if (v.time != 0) {
			  result++;
		  }
	  }
	  return result;
  }
protected:
  Id _target_id;
  StepKind _step;
  std::vector<Meas> _values;
  uint64_t _period;
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
		  logger_fatal("engine: bystep - write values #", value.id, " with unknow step.");
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
	  logger_info("engine: bystep - period #", value.id, " is ", period_num);
	  if (ptr->period() != period_num) {
		  auto old_value = ptr;
		  logger_info("engine: bystep - new period for #", value.id, " old size: ", old_value->size());
		  ptr = ByStepTrack_ptr{ new ByStepTrack(value.id, stepKind_it->second, period_num) };
		  _values[value.id] = ptr;
	  }
	  auto res = ptr->append(value);
	  if (res.writed != 1) {
		  THROW_EXCEPTION("engine- logic_error.");
	  }
	  return Status(1, 0); 
  }

  Id2MinMax loadMinMax() override {
    Id2MinMax result;
    return result;
  }

  Time minTime() override {
    Time result = MAX_TIME;
    return result;
  }

  Time maxTime() override {
    Time result = MIN_TIME;
    return result;
  }

  bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                  dariadb::Time *maxResult) override {
    return false;
  }

  void foreach (const QueryInterval &q, IReaderClb * clbk) override { NOT_IMPLEMENTED; }

  Id2Meas readTimePoint(const QueryTimePoint &q) override { return Id2Meas(); }

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