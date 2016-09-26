#include "options.h"
#include "../utils/exception.h"
#include "../utils/fs.h"
#include "../utils/logger.h"
#include <extern/json/json.hpp>
#include <fstream>

using namespace dariadb::storage;
using json = nlohmann::json;

const size_t AOF_BUFFER_SIZE = 2000;
const size_t AOF_FILE_SIZE = AOF_BUFFER_SIZE*2;
const uint32_t OPENNED_PAGE_CACHE_SIZE = 10;
const uint32_t CHUNK_SIZE = 1024;

Options *Options::_instance = nullptr;

std::string options_file_path(const std::string &path) {
  return dariadb::utils::fs::append_path(path, OPTIONS_FILE_NAME);
}

Options::Options() {
  set_default();

  strategy = STRATEGY::COMPRESSED;
}

void Options::start() {
  if (_instance == nullptr) {
    _instance = new Options();
  }
}

void Options::start(const std::string &path) {
  Options::start();
  Options::instance()->path = path;
  auto f = options_file_path(path);
  if (utils::fs::path_exists(f)) {
    Options::instance()->load(f);
  }
}

void Options::set_default() {
  logger("engine: options set default options");
  aof_buffer_size = AOF_BUFFER_SIZE;
  aof_max_size = AOF_FILE_SIZE;
  page_chunk_size = CHUNK_SIZE;
  page_openned_page_cache_size = OPENNED_PAGE_CACHE_SIZE;

  strategy = STRATEGY::COMPRESSED;
}

void Options::stop() {
  delete _instance;
  _instance = nullptr;
}

std::vector<dariadb::utils::async::ThreadPool::Params> Options::thread_pools_params() {
  using namespace dariadb::utils::async;
  std::vector<ThreadPool::Params> result{
      ThreadPool::Params{size_t(4), (ThreadKind)THREAD_COMMON_KINDS::READ},
      ThreadPool::Params{size_t(3), (ThreadKind)THREAD_COMMON_KINDS::FILE_READ},
      ThreadPool::Params{size_t(1), (ThreadKind)THREAD_COMMON_KINDS::DROP}};
  return result;
}

void Options::save() {
  save(options_file_path(_instance->path));
}

void Options::save(const std::string &file) {
  logger("engine: options save to ", file);
  json js;

  js["aof_max_size"] = aof_max_size;
  js["aof_buffer_size"] = aof_buffer_size;

  js["page_chunk_size"] = page_chunk_size;
  js["page_openned_page_cache_size"] = page_openned_page_cache_size;

  std::stringstream ss;
  ss << strategy;
  js["stragety"] = ss.str();

  std::fstream fs;
  fs.open(file, std::ios::out);
  if (!fs.is_open()) {
    throw MAKE_EXCEPTION("!fs.is_open()");
  }
  fs << js.dump();
  fs.flush();
  fs.close();
}

void Options::load(const std::string &file) {
  logger("engine: options loading ", file);
  std::string content = dariadb::utils::fs::read_file(file);
  json js = json::parse(content);
  aof_max_size = js["aof_max_size"];
  aof_buffer_size = js["aof_buffer_size"];

  page_chunk_size = js["page_chunk_size"];
  page_openned_page_cache_size = js["page_openned_page_cache_size"];

  std::istringstream iss;
  std::string strat_str = js["stragety"];
  iss.str(strat_str);
  iss >> strategy;
}
