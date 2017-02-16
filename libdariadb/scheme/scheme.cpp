#include <fstream>
#include <libdariadb/scheme/helpers.h>
#include <libdariadb/scheme/scheme.h>
#include <libdariadb/utils/async/locker.h>
#include <libdariadb/utils/fs.h>
#include <libdariadb/utils/logger.h>
#include <unordered_map>

#include <extern/json/src/json.hpp>

#include <boost/regex.hpp>

const std::string SCHEME_FILE_NAME = "scheme.js";

const std::string key_params = "params";
const std::string key_name = "name";
const std::string key_id = "id";

using namespace dariadb;
using namespace dariadb::scheme;

using json = nlohmann::json;

struct Scheme::Private : public IScheme {
  Private(const storage::Settings_ptr s) : _settings(s) {
    _next_id = 0;
    load();
  }

  std::string schemeFile() const {
    return utils::fs::append_path(_settings->storage_path.value(),
                                  SCHEME_FILE_NAME);
  }

  Id addParam(const std::string &param) override {
    std::lock_guard<utils::async::Locker> lg(_locker);

    auto fres = _params.find(param);
    if (fres != _params.end()) {
      return fres->second.id;
    }
    auto id = _next_id++;
    this->_params[param] = MeasurementDescription{param, id};
    return id;
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
    auto file = schemeFile();
    logger("scheme: save to ", file);
    json js;

    for (auto &o : _params) {
      json reccord = {{key_name, o.first}, {key_id, o.second.id}};
      js[key_params].push_back(reccord);
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
    auto file = schemeFile();
    logger_info("scheme: loading ", file);

    if (!utils::fs::file_exists(file)) {
      logger_info("scheme: file not found.");
      return;
    }
    std::string content = dariadb::utils::fs::read_file(file);
    json js = json::parse(content);
    auto params_array = js[key_params];
    Id max_id = 0;
    for (auto kv : params_array) {
      auto param_name = kv[key_name].get<std::string>();
      auto param_id = kv[key_id].get<Id>();

      _params[param_name] = MeasurementDescription{param_name, param_id};
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

Scheme_Ptr Scheme::create(const storage::Settings_ptr s) {
  return std::shared_ptr<Scheme>(new Scheme(s));
}

Scheme::Scheme(const storage::Settings_ptr s) : _impl(new Scheme::Private(s)) {}

Id Scheme::addParam(const std::string &param) { return _impl->addParam(param); }

DescriptionMap Scheme::ls() { return _impl->ls(); }

void Scheme::save() { _impl->save(); }
