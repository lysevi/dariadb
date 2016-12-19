#include <libdariadb/flags.h>
#include <libdariadb/storage/bystep/bystep_storage.h>
#include <libdariadb/storage/bystep/bysteptrack.h>
#include <libdariadb/storage/bystep/helpers.h>
#include <libdariadb/storage/bystep/io_adapter.h>
#include <libdariadb/storage/chunk.h>
#include <libdariadb/storage/settings.h>
#include <libdariadb/timeutil.h>
#include <libdariadb/utils/fs.h>
#include <cassert>
#include <cstring>
#include <memory>
#include <shared_mutex>
#include <tuple>
#include <unordered_map>
#include <vector>

using namespace dariadb;
using namespace dariadb::storage;
using namespace dariadb::storage::bystep;

const char *filename = "bystep.db";

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
      std::lock_guard<std::shared_mutex> lg(_values_lock);
      for (auto &kv : _values) {
        write_track_to_disk(kv.second);
      }
      _io->stop();
      _io = nullptr;
    }
  }

  ~Private() { stop(); }

  size_t set_steps(const Id2Step &steps) {
    std::lock_guard<std::shared_mutex> lg(_values_lock);
    _steps = steps;
    _values.reserve(steps.size());

    logger("engine: bystep - load chunks for current time.");
    size_t result = 0;
    // load current period.
    auto cur_time = timeutil::current_time();
    for (auto &kv : _steps) {
      auto vals_per_interval = bystep::step_to_size(kv.second);
      auto period_num = bystep::intervalForTime(kv.second, vals_per_interval, cur_time);
      auto chunk = _io->readTimePoint(period_num, kv.first);
      if (chunk != nullptr) {
        auto track = ByStepTrack_ptr{new ByStepTrack(kv.first, kv.second, period_num)};
        track->from_chunk(chunk);
        track->must_be_replaced = true;
        track->was_updated = false;
        _values[kv.first] = track;
        ++result;
      }
    }
    return result;
  }

  Status append(const Meas &value) override {
    auto stepKind_it = _steps.find(value.id);
    if (stepKind_it == _steps.end()) {
      logger_fatal("engine: bystep - write values id:", value.id, " with unknow step.");
      return Status(0, 1);
    }

    auto vals_per_interval = bystep::step_to_size(stepKind_it->second);
    auto period_num =
        bystep::intervalForTime(stepKind_it->second, vals_per_interval, value.time);

    _values_lock.lock_shared();
    auto it = _values.find(value.id);
    ByStepTrack_ptr ptr = nullptr;
    if (it == _values.end()) {
      _values_lock.unlock_shared();
      std::lock_guard<std::shared_mutex> lg(_values_lock);
      it = _values.find(value.id);
      if (it == _values.end()) {
        ptr = ByStepTrack_ptr{new ByStepTrack(value.id, stepKind_it->second, period_num)};
        _values.emplace(std::make_pair(value.id, ptr));
      } else {
        ptr = it->second;
      }
    } else {
      ptr = it->second;
      _values_lock.unlock_shared();
    }

    if (ptr->period() < period_num) { // new storage period.
      write_track_to_disk(ptr);
      std::lock_guard<std::shared_mutex> lg(_values_lock);
      ptr = ByStepTrack_ptr{new ByStepTrack(value.id, stepKind_it->second, period_num)};
      _values[value.id] = ptr;
    }

    if (ptr->period() > period_num) { // write to past. replace old.
      auto track =
          ByStepTrack_ptr{new ByStepTrack(value.id, stepKind_it->second, period_num)};
      auto ch = _io->readTimePoint(period_num, value.id);
      if (ch != nullptr) {
        track->from_chunk(ch);
      }
      track->append(value);
      track->must_be_replaced = true;
      write_track_to_disk(track);
    } else {
      auto res = ptr->append(value);
      if (res.writed != 1) {
        THROW_EXCEPTION("engine: bystep - logic_error.");
      }
    }
    return Status(1, 0);
  }

  void write_track_to_disk(const ByStepTrack_ptr &track) {
    if (track->was_updated) {
      std::lock_guard<std::mutex> lg(_drop_lock);
      auto packed_chunk = track->pack();
      if (packed_chunk != nullptr) {
        // TODO write async.
        if (!track->must_be_replaced) {
          _io->append(packed_chunk, track->minTime(), track->maxTime());
        } else {
          _io->replace(packed_chunk, track->minTime(), track->maxTime());
        }
      }
    }
  }

  Id2MinMax loadMinMax() override {
    Id2MinMax result;
    std::shared_lock<std::shared_mutex> lg(_values_lock);
    for (auto &kv : _values) {
      auto mm = kv.second->loadMinMax();
      // TODO read from disk to
      if (mm.size() != 0) {
        result[kv.first] = mm[kv.first];
      }
    }
    return result;
  }

  Time minTime() override {
    std::shared_lock<std::shared_mutex> lg(_values_lock);
    Time result = _io->minTime();
    for (auto &kv : _values) {
      result = std::min(result, kv.second->minTime());
    }
    return result;
  }

  Time maxTime() override {
    std::shared_lock<std::shared_mutex> lg(_values_lock);
    Time result = _io->maxTime();
    for (auto &kv : _values) {
      result = std::max(result, kv.second->maxTime());
    }
    return result;
  }

  bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                  dariadb::Time *maxResult) override {
    std::shared_lock<std::shared_mutex> lg(_values_lock);
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
    std::shared_lock<std::shared_mutex> lg(_values_lock);
    QueryInterval local_q = q;
    local_q.ids.resize(1);
    for (auto id : q.ids) {
      auto step_kind_it = _steps.find(id);
      if (step_kind_it == _steps.end()) {
        logger_fatal("engine: bystep - unknow id:", id);
        continue;
      }

      auto round_from = bystep::roundTime(step_kind_it->second, q.from);
      auto stepTime = std::get<1>(round_from);
      auto round_to = bystep::roundTime(step_kind_it->second, q.to + stepTime);

      auto vals_per_interval = bystep::step_to_size(step_kind_it->second);

      auto period_from = bystep::intervalForTime(step_kind_it->second, vals_per_interval,
                                                 std::get<0>(round_from));
      auto period_to = bystep::intervalForTime(step_kind_it->second, vals_per_interval,
                                               std::get<0>(round_to));

      local_q.ids[0] = id;
      local_q.from = std::get<0>(round_from);
      local_q.to = std::get<0>(round_to);
      auto readed_chunks = _io->readInterval(period_from, period_to, id);

      for (auto i = period_from; i < period_to; ++i) {
        if (!readed_chunks.empty() && readed_chunks.front()->header->id == i) {
          auto c = readed_chunks.front();
          readed_chunks.pop_front();
          foreach_chunk(q, clbk, c);
        } else {
          auto values_size = bystep::step_to_size(step_kind_it->second);
          auto zero_time = ByStepTrack::get_zero_time(i, step_kind_it->second);
          auto empty_value = Meas::empty(id);
          for (size_t meas_num = 0; meas_num < values_size; ++meas_num) {
            empty_value.time = zero_time;
            empty_value.flag = Flags::_NO_DATA;
            zero_time += stepTime;
          }
          ByStepTrack tr(id, step_kind_it->second, i);
          tr.foreach (local_q, clbk);
        }
      }

      auto it = _values.find(id);
      if (it != _values.end()) {
        it->second->foreach (local_q, clbk);
      }
    }
  }

  void foreach_chunk(const QueryInterval &q, IReaderClb *clbk, const Chunk_Ptr &c) {
    auto rdr = c->getReader();
    while (!rdr->is_end()) {
      auto v = rdr->readNext();
      if (v.time >= q.from && v.time < q.to) {
        if (!v.inFlag(q.flag)) {
          v.flag = Flags::_NO_DATA;
        }
        clbk->call(v);
      }
    }
  }

  Id2Meas readTimePoint(const QueryTimePoint &q) override {
    std::shared_lock<std::shared_mutex> lg(_values_lock);
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
      auto round_tp = bystep::roundTime(stepKind_it->second, q.time_point);

      local_q.ids[0] = id;
      local_q.time_point = std::get<0>(round_tp);

      result[id].time = local_q.time_point;
      result[id].flag = Flags::_NO_DATA;

      auto vals_per_interval = bystep::step_to_size(stepKind_it->second);
      auto period_num = bystep::intervalForTime(stepKind_it->second, vals_per_interval,
                                                local_q.time_point);

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
            auto rounded_time_tuple = bystep::roundTime(stepKind_it->second, v.time);
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
    Id2Meas result;
    std::shared_lock<std::shared_mutex> lg(_values_lock);
    for (auto &kv : _values) {
      auto mm = kv.second->currentValue({}, flag);
      // TODO read from disk to
      if (mm.size() != 0) {
        result[kv.first] = mm[kv.first];
      }
    }
    return result;
  }

  void flush() override {}

  Description description() {
	  Description result;
	  result.in_queue = _io->description().in_queue;
	  return result;
  }
  EngineEnvironment_ptr _env;
  storage::Settings *_settings;
  std::unique_ptr<IOAdapter> _io;
  std::unordered_map<Id, ByStepTrack_ptr> _values;
  std::shared_mutex _values_lock;
  std::mutex _drop_lock;
  Id2Step _steps;
};

ByStepStorage::ByStepStorage(const EngineEnvironment_ptr &env)
    : _impl(new ByStepStorage::Private(env)) {}

ByStepStorage::~ByStepStorage() {
  _impl = nullptr;
}

size_t ByStepStorage::set_steps(const Id2Step &steps) {
  return _impl->set_steps(steps);
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

uint64_t ByStepStorage::intervalForTime(const STEP_KIND step, const size_t valsInInterval,
                                        const Time t) {
  return bystep::intervalForTime(step, valsInInterval, t);
}

ByStepStorage::Description ByStepStorage::description() {
	return _impl->description();
}