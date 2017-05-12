#include <libdariadb/scheme/helpers.h>
#include <libdariadb/scheme/scheme.h>
#include <libdariadb/statistic/calculator.h>
#include <libdariadb/timeutil.h>
#include <libdariadb/utils/async/locker.h>
#include <libdariadb/utils/fs.h>
#include <libdariadb/utils/logger.h>
#include <fstream>
#include <unordered_map>

#include <extern/json/src/json.hpp>

#include <regex>

const std::string SCHEME_FILE_NAME = "scheme.js";

const std::string scheme_key_params = "params";
const std::string scheme_key_name = "name";
const std::string scheme_key_id = "id";
const std::string scheme_key_interval = "interval";
const std::string scheme_key_afunction = "aggregation_func";

using namespace dariadb;
using namespace dariadb::scheme;

using json = nlohmann::json;

struct Scheme::Private : public IScheme {
  using Param2Description = std::unordered_map<std::string, MeasurementDescription>;
  Private(const storage::Settings_ptr s) : _settings(s) {
    _next_id = 0;
    load();
  }

  std::string schemeFile() const {
    if (_settings->is_memory_only_mode) {
      return storage::MEMORY_ONLY_PATH;
    }
    return utils::fs::append_path(_settings->storage_path.value(), SCHEME_FILE_NAME);
  }

  bool is_interval(const std::string &s) {
    if (s == "raw") {
      return true;
    }
    auto predefined = timeutil::predefinedIntervals();
    for (auto v : predefined) {
      if (s == v) {
        return true;
      }
    }
    return false;
  }

  bool is_aggregate_fn(const std::string &s) {
    auto all_functions = dariadb::statistic::FunctionFactory::functions();
    for (auto fn : all_functions) {
      if (fn == s) {
        return true;
      }
    }
    return false;
  }

  Id addParam(const std::string &param) override {
    std::lock_guard<utils::async::Locker> lg(_locker);

    auto splited = utils::strings::split(param, '.');
    MeasurementDescription md;
    md.name = param;

    if (splited.size() > 1) {
      if (is_interval(splited.back())) {
        md.interval = splited.back();
      }
      if (splited.size() >= 3) { // may contain aggregate functions name
        auto rbegin = splited.rbegin();
        rbegin++;
        if (is_aggregate_fn(*rbegin)) {
          md.aggregation_func = *rbegin;
        }
      }
    }

    auto interval_fres = _params.find(md.interval);
    if (interval_fres != _params.end()) {
      auto fres = interval_fres->second.find(md.name);
      if (fres != interval_fres->second.end()) {
        return fres->second.id;
      }
    }
    md.id = _next_id++;
    this->_params[md.interval][param] = md;
    _id2descr[md.id] = md;
    return md.id;
  }

  DescriptionMap ls() override {
    std::lock_guard<utils::async::Locker> lg(_locker);
    DescriptionMap result;
    for (auto &inter : _params) {
      for (const auto &kv : inter.second) {
        result[kv.second.id] = kv.second;
      }
    }
    return result;
  }

  MeasurementDescription descriptionFor(dariadb::Id id) override {
    std::lock_guard<utils::async::Locker> lg(_locker);
    MeasurementDescription result;
    auto fiter = _id2descr.find(id);
    if (fiter == _id2descr.end()) {
      result.id = MAX_ID;
    } else {
      result = fiter->second;
    }
    return result;
  }

  DescriptionMap lsInterval(const std::string &interval) {
    std::lock_guard<utils::async::Locker> lg(_locker);
    DescriptionMap result;
    auto iter = _params.find(interval);
    if (iter != _params.end()) {
      for (auto kv : iter->second) {
        result[kv.second.id] = kv.second;
      }
    }
    return result;
  }

  DescriptionMap linkedForValue(const MeasurementDescription &param) {
    std::lock_guard<utils::async::Locker> lg(_locker);
    DescriptionMap result;
    std::vector<std::string> all_intervals;
    for (auto &kv : _params) {
      all_intervals.push_back(kv.first);
    }
    std::sort(all_intervals.begin(), all_intervals.end(),
              [](auto &r, auto &l) { return timeutil::intervalsLessCmp(r, l); });

    auto source_prefix = param.prefix();
    for (size_t i = 0; i < (all_intervals.size() - 1); ++i) {
      if (all_intervals[i] == param.interval) {
        auto target_inteval = all_intervals[i + 1];
        auto all_from_target = _params[target_inteval];
        for (auto kv : all_from_target) {
          auto candidate_prefix = kv.second.prefix();
          if (candidate_prefix.compare(0, source_prefix.size(), source_prefix) == 0) {
            auto new_value = std::make_pair(kv.second.id, kv.second);
            result.insert(new_value);
          }
        }
      }
    }
    return result;
  }

  void save() {
    if (_settings->is_memory_only_mode) {
      return;
    }
    auto file = schemeFile();
    logger("scheme: save to ", file);
    json js;

    for (const auto &interval : _params) {
      for (auto &o : interval.second) {
        json reccord = {{scheme_key_name, o.first},
                        {scheme_key_id, o.second.id},
                        {scheme_key_interval, o.second.interval},
                        {scheme_key_afunction, o.second.aggregation_func}};
        js[scheme_key_params].push_back(reccord);
      }
    }

    std::fstream fs;
    fs.open(file, std::ios::out);
    if (!fs.is_open()) {
      throw MAKE_EXCEPTION("!fs.is_open()");
    }
    fs << js.dump(1);
    fs.flush();
    fs.close();
  }

  void load() {
    if (_settings->is_memory_only_mode) {
      return;
    }
    auto file = schemeFile();
    logger_info("scheme: loading ", file);

    if (!utils::fs::file_exists(file)) {
      logger_info("scheme: file not found.");
      return;
    }
    std::string content = dariadb::utils::fs::read_file(file);
    json js = json::parse(content);
    auto params_array = js[scheme_key_params];
    Id max_id = 0;

    for (auto kv : params_array) {
      auto param_name = kv[scheme_key_name].get<std::string>();
      auto param_id = kv[scheme_key_id].get<Id>();
      auto param_interval = kv[scheme_key_interval].get<std::string>();
      auto param_function = kv[scheme_key_afunction].get<std::string>();

      MeasurementDescription descr{param_name, param_id, param_interval, param_function};
      _params[param_interval].insert(std::make_pair(param_name, descr));
      _id2descr[descr.id] = descr;
      max_id = std::max(max_id, param_id);
    }
    _next_id = max_id + 1;
    logger("scheme: ", _params.size(), " params loaded.");
  }

  storage::Settings_ptr _settings;

  mutable dariadb::utils::async::Locker _locker;
  std::unordered_map<std::string, Param2Description> _params;
  std::unordered_map<Id, MeasurementDescription> _id2descr;
  Id _next_id;
};

IScheme_Ptr Scheme::create(const storage::Settings_ptr s) {
  return std::shared_ptr<Scheme>(new Scheme(s));
}

Scheme::Scheme(const storage::Settings_ptr s) : _impl(new Scheme::Private(s)) {}

Id Scheme::addParam(const std::string &param) {
  return _impl->addParam(param);
}

DescriptionMap Scheme::ls() {
  return _impl->ls();
}

MeasurementDescription Scheme::descriptionFor(dariadb::Id id) {
  return _impl->descriptionFor(id);
}

DescriptionMap Scheme::lsInterval(const std::string &interval) {
  return _impl->lsInterval(interval);
}

DescriptionMap Scheme::linkedForValue(const MeasurementDescription &param) {
  return _impl->linkedForValue(param);
}
void Scheme::save() {
  _impl->save();
}
