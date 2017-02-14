#include <fstream>
#include <iostream>
#include <libdariadb/scheme/helpers.h>
#include <libdariadb/scheme/scheme.h>
#include <libdariadb/utils/async/locker.h>
#include <libdariadb/utils/fs.h>
#include <libdariadb/utils/logger.h>

#include <extern/json/src/json.hpp>

const std::string SCHEME_FILE_NAME = "scheme.js";

const std::string key_params = "params";
const std::string key_name = "name";
const std::string key_id = "id";

using namespace dariadb;
using namespace dariadb::scheme;

using json = nlohmann::json;

struct Scheme::Private : public IScheme {
  Private(const storage::Settings_ptr s) : _settings(s) {
    load();
    _next_id = 0;
  }

  std::string schemeFile() const {
    return utils::fs::append_path(_settings->storage_path.value(),
                                  SCHEME_FILE_NAME);
  }

  void addParam(const std::string &param) override {
    std::lock_guard<utils::async::Locker> lg(_locker);
    MeasurementDescription md;
    md.name = param;
    md.id = _next_id++;
    this->_params.push_back(md);
  }

  std::list<MeasurementDescription> ls() override {
    std::lock_guard<utils::async::Locker> lg(_locker);
    std::list<MeasurementDescription> result(_params.begin(), _params.end());
    return result;
  }

  std::list<MeasurementDescription> ls(const std::string &pattern) override {
    std::lock_guard<utils::async::Locker> lg(_locker);
    std::list<MeasurementDescription> result;
    return result;
  }

  void save() {
    auto file = schemeFile();
    logger("scheme: save to ", file);
    json js;

    for (auto &o : _params) {
      json reccord = {{key_name, o.name}, {key_id, o.id}};
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
    for (auto kv : params_array) {
      auto param_name = kv[key_name].get<std::string>();
      auto param_id = kv[key_id].get<Id>();
	  MeasurementDescription md;
	  md.name = param_name;
	  md.id = param_id;
	  _params.push_back(md);
    }

	logger("scheme: ", _params.size(), " params loaded.");
  }
  storage::Settings_ptr _settings;

  dariadb::utils::async::Locker _locker;
  std::list<MeasurementDescription> _params;
  Id _next_id;
};

Scheme_Ptr Scheme::create(const storage::Settings_ptr s) {
  return std::shared_ptr<Scheme>(new Scheme(s));
}

Scheme::Scheme(const storage::Settings_ptr s) : _impl(new Scheme::Private(s)) {}

void Scheme::addParam(const std::string &param) { _impl->addParam(param); }

std::list<MeasurementDescription> Scheme::ls() { return _impl->ls(); }

std::list<MeasurementDescription> Scheme::ls(const std::string &pattern) {
  return _impl->ls(pattern);
}

void Scheme::save() { _impl->save(); }