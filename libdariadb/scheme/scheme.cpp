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

    auto fres = _params.find(param);
    if (fres != _params.end()) {
      return fres->second.id;
    }
    md.id = _next_id++;
    this->_params[param] = md;
    return md.id;
  }

  DescriptionMap ls() override {
    std::lock_guard<utils::async::Locker> lg(_locker);
    DescriptionMap result;
    for (auto kv : _params) {
      result[kv.second.id] = kv.second;
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

    for (auto &o : _params) {
      json reccord = {{scheme_key_name, o.first},
                      {scheme_key_id, o.second.id},
                      {scheme_key_interval, o.second.interval},
                      {scheme_key_afunction, o.second.aggregation_func}};
      js[scheme_key_params].push_back(reccord);
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

      _params[param_name] =
          MeasurementDescription{param_name, param_id, param_interval, param_function};
      max_id = std::max(max_id, param_id);
    }
    _next_id = max_id + 1;
    logger("scheme: ", _params.size(), " params loaded.");
  }

  storage::Settings_ptr _settings;

  dariadb::utils::async::Locker _locker;
  std::unordered_map<std::string, MeasurementDescription> _params;
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

void Scheme::save() {
  _impl->save();
}
