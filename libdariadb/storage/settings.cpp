#include <extern/json/src/json.hpp>
#include <fstream>
#include <libdariadb/meas.h>
#include <libdariadb/storage/settings.h>
#include <libdariadb/utils/exception.h>
#include <libdariadb/utils/fs.h>
#include <libdariadb/utils/logger.h>
#include <libdariadb/utils/strings.h>

using namespace dariadb::storage;
using json = nlohmann::json;

const size_t AOF_BUFFER_SIZE = 2000;
const size_t AOF_FILE_SIZE = sizeof(dariadb::Meas) * AOF_BUFFER_SIZE * 4;
const uint32_t CHUNK_SIZE = 1024;
const size_t MAXIMUM_MEMORY_LIMIT = 100 * 1024 * 1024; //10 mb

const std::string c_aof_max_size="aof_max_size";
const std::string c_aof_buffer_size="aof_buffer_size";
const std::string c_chunk_size="chunk_size";
const std::string c_strategy="strategy";
const std::string c_memory_limit="memory_limit";
const std::string c_percent_when_start_droping="percent_when_start_droping";
const std::string c_percent_to_drop="percent_to_drop";

std::string settings_file_path(const std::string &path) {
  return dariadb::utils::fs::append_path(path, SETTINGS_FILE_NAME);
}

Settings::Settings(const std::string storage_path) {
  path = storage_path;
  auto f = settings_file_path(path);
  if (utils::fs::path_exists(f)) {
    load(f);
  } else {
	dariadb::utils::fs::mkdir(path);
    set_default();
    save();
  }
}

Settings::~Settings(){}

void Settings::set_default() {
  logger("engine: Settings set default Settings");
  aof_buffer_size = AOF_BUFFER_SIZE;
  aof_max_size = AOF_FILE_SIZE;
  chunk_size = CHUNK_SIZE;
  memory_limit = MAXIMUM_MEMORY_LIMIT;
  strategy = STRATEGY::COMPRESSED;
  percent_when_start_droping = float(0.75);
  percent_to_drop = float(0.15);
}

std::vector<dariadb::utils::async::ThreadPool::Params> Settings::thread_pools_params() {
  using namespace dariadb::utils::async;
  std::vector<ThreadPool::Params> result{
      ThreadPool::Params{size_t(4), (ThreadKind)THREAD_COMMON_KINDS::COMMON},
      ThreadPool::Params{size_t(1), (ThreadKind)THREAD_COMMON_KINDS::DISK_IO},
      ThreadPool::Params{size_t(1), (ThreadKind)THREAD_COMMON_KINDS::DROP}};
  return result;
}

void Settings::save() {
  save(settings_file_path(this->path));
}

void Settings::save(const std::string &file) {
  logger("engine: Settings save to ", file);
  json js;

  js[c_aof_max_size] = aof_max_size;
  js[c_aof_buffer_size] = aof_buffer_size;

  js[c_chunk_size] = chunk_size;

  std::stringstream ss;
  ss << strategy;
  js[c_strategy] = ss.str();

  js[c_memory_limit] = memory_limit;
  js[c_percent_when_start_droping] = percent_when_start_droping;
  js[c_percent_to_drop] = percent_to_drop;
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
  aof_max_size = js[c_aof_max_size];
  aof_buffer_size = js[c_aof_buffer_size];

  chunk_size = js[c_chunk_size];

  memory_limit=js[c_memory_limit];
  percent_when_start_droping=js[c_percent_when_start_droping] ;
  percent_to_drop= js[c_percent_to_drop];

  std::istringstream iss;
  std::string strat_str = js[c_strategy];
  iss.str(strat_str);
  iss >> strategy;
}

std::string Settings::dump(){
   auto content=dariadb::utils::fs::read_file(settings_file_path(path));
   json js = json::parse(content);
   std::stringstream ss;
   ss<<js.dump(1)<<std::endl;
   return ss.str();
}

void Settings::change(std::string& expression){
    auto splited=utils::strings::split(expression,'=');
    if(splited.size()!=2){
        THROW_EXCEPTION("bad format. use: name=value");
    }
    if(splited[0]==c_aof_max_size){
        logger_info("engine: change ", c_aof_max_size);
        this->aof_max_size = std::stoll(splited[1]);
        return;
    }

    if(splited[0]==c_aof_buffer_size){
        logger_info("engine: change ", c_aof_buffer_size);
        this->aof_buffer_size = std::stoi(splited[1]);
        return;
    }

    if(splited[0]==c_chunk_size){
        logger_info("engine: change ", c_chunk_size);
        this->chunk_size = std::stoi(splited[1]);
        return;
    }

    if(splited[0]==c_strategy){
        logger_info("engine: change ", c_strategy);
        std::istringstream iss(splited[1]);
        iss>>this->strategy;
        return;
    }

    if(splited[0]==c_memory_limit){
        logger_info("engine: change ", c_memory_limit);
        this->memory_limit = std::stoi(splited[1]);
        return;
    }
	if (splited[0] == c_percent_when_start_droping) {
		logger_info("engine: change ", c_percent_when_start_droping);
		this->percent_when_start_droping = std::stof(splited[1]);
		return;
	}

    if(splited[0]==c_percent_to_drop){
        logger_info("engine: change ", c_percent_to_drop);
        this->percent_to_drop = std::stof(splited[1]);
        return;
    }

    logger_fatal("engine: engine: bad expression ", expression);
}
