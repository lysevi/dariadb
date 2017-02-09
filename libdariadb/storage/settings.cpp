#include <extern/json/src/json.hpp>
#include <fstream>
#include <libdariadb/meas.h>
#include <libdariadb/storage/settings.h>
#include <libdariadb/utils/exception.h>
#include <libdariadb/utils/fs.h>
#include <libdariadb/utils/logger.h>
#include <libdariadb/utils/strings.h>
#include <sstream>

using namespace dariadb::storage;
using namespace dariadb::utils;

using json = nlohmann::json;

const uint64_t WAL_CACHE_SIZE = 4096 / sizeof(dariadb::Meas) * 10;
const uint64_t WAL_FILE_SIZE = (1024 * 1024) * 4 / sizeof(dariadb::Meas);
const uint32_t CHUNK_SIZE = 1024;
const size_t MAXIMUM_MEMORY_LIMIT = 100 * 1024 * 1024; // 100 mb

const std::string c_wal_file_size = "wal_file_size";
const std::string c_wal_cache_size = "wal_cache_size";
const std::string c_chunk_size = "chunk_size";
const std::string c_strategy = "strategy";
const std::string c_memory_limit = "memory_limit";
const std::string c_percent_when_start_droping = "percent_when_start_droping";
const std::string c_percent_to_drop = "percent_to_drop";
const std::string c_max_pages_per_level = "max_pages_per_level";

std::string settings_file_path(const std::string &path) {
  return dariadb::utils::fs::append_path(path, SETTINGS_FILE_NAME);
}

template <class T>
void Settings::ReadOnlyOption<T>::from_string(const std::string &s) {
  std::istringstream iss(s);
  iss >> _value;
}

template <>
std::string Settings::ReadOnlyOption<dariadb::STRATEGY>::value_str() const {
  return dariadb::to_string(this->value());
}
template <>
std::string Settings::ReadOnlyOption<std::string>::value_str() const {
  return this->value();
}

BaseOption::~BaseOption() {}

Settings_ptr Settings::create(const std::string &storage_path) {
  return Settings_ptr{new Settings(storage_path)};
}

Settings::Settings(const std::string &path_to_storage)
    : storage_path(nullptr, "storage path", path_to_storage),
      raw_path(nullptr, "raw path", fs::append_path(path_to_storage, "raw")),
      wal_file_size(this, c_wal_file_size, WAL_FILE_SIZE),
      wal_cache_size(this, c_wal_cache_size, WAL_CACHE_SIZE),
      chunk_size(this, c_chunk_size, CHUNK_SIZE),
      strategy(this, c_strategy, STRATEGY::COMPRESSED),
      memory_limit(this, c_memory_limit, MAXIMUM_MEMORY_LIMIT),
      percent_when_start_droping(this, c_percent_when_start_droping,
                                 float(0.75)),
      percent_to_drop(this, c_percent_to_drop, float(0.1)),
      max_pages_in_level(this, c_max_pages_per_level, uint16_t(2)) {
  auto f = settings_file_path(storage_path.value());
  if (utils::fs::path_exists(f)) {
    load(f);
  } else {
    dariadb::utils::fs::mkdir(storage_path.value());
    dariadb::utils::fs::mkdir(raw_path.value());
    save();
  }
  load_min_max = true;
}

Settings::~Settings() {}

void Settings::set_default() {
  logger("engine: Settings set default Settings");
  wal_cache_size.setValue(WAL_CACHE_SIZE);
  wal_file_size.setValue(WAL_FILE_SIZE);
  chunk_size.setValue(CHUNK_SIZE);
  memory_limit.setValue(MAXIMUM_MEMORY_LIMIT);
  strategy.setValue(STRATEGY::COMPRESSED);
  percent_when_start_droping.setValue(float(0.75));
  percent_to_drop.setValue(float(0.15));
}

std::vector<dariadb::utils::async::ThreadPool::Params>
Settings::thread_pools_params() {
  using namespace dariadb::utils::async;
  std::vector<ThreadPool::Params> result{
      ThreadPool::Params{size_t(4), (ThreadKind)THREAD_KINDS::COMMON},
      ThreadPool::Params{size_t(1), (ThreadKind)THREAD_KINDS::DISK_IO}};
  return result;
}

void Settings::save() { save(settings_file_path(this->storage_path.value())); }

void Settings::save(const std::string &file) {
  logger("engine: Settings save to ", file);
  json js;

  for (auto &o : _all_options) {
    js[o.first] = o.second->value_str();
  }

  std::fstream fs;
  fs.open(file, std::ios::out);
  if (!fs.is_open()) {
    throw MAKE_EXCEPTION("!fs.is_open()");
  }
  fs << js.dump();
  fs.flush();
  fs.close();
}

void Settings::load(const std::string &file) {
  logger("engine: Settings loading ", file);
  std::string content = dariadb::utils::fs::read_file(file);
  json js = json::parse(content);
  for (auto &o : _all_options) {
    auto str_val = js[o.first];
    o.second->from_string(str_val);
  }
}

std::string Settings::dump() {
  auto content =
      dariadb::utils::fs::read_file(settings_file_path(storage_path.value()));
  json js = json::parse(content);
  std::stringstream ss;
  ss << js.dump(1) << std::endl;
  return ss.str();
}

void Settings::change(std::string &expression) {
  auto splited = utils::strings::split(expression, '=');
  if (splited.size() != 2) {
    THROW_EXCEPTION("bad format. use: name=value");
  }

  auto fres = _all_options.find(splited[0]);
  if (fres != _all_options.end()) {
    logger_info("engine: change ", fres->first);
    fres->second->from_string(splited[1]);
  } else {
    logger_fatal("engine: engine: bad expression ", expression);
  }
}
