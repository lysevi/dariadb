#pragma once

#include <libdariadb/meas.h>
#include <libdariadb/utils/locker.h>
#include <libdariadb/utils/thread_pool.h>
#include <libdariadb/storage/strategy.h>
#include <libdariadb/st_exports.h>

#include <unordered_map>
#include <string>
namespace dariadb {
namespace storage {

const std::string SETTINGS_FILE_NAME = "Settings";
class BaseOption {
public:
	EXPORT virtual ~BaseOption();
	virtual std::string key_str()const =0;
	virtual std::string value_str()const =0;
	virtual void from_string(const std::string&s)=0;
};

class Settings {
  std::unordered_map<std::string, BaseOption *> _all_options;

  template <typename T> class Option : public BaseOption {
  public:
    Option() = delete;
    Option(Settings *s, const std::string &keyname, const T default_value)
        : key_name(keyname), value(default_value) {

      auto sres = s->_all_options.find(keyname);
      if (sres == s->_all_options.end()) {
        s->_all_options.emplace(keyname, this);
      } else {
        THROW_EXCEPTION("Option duplicate key.");
      }
    }

    std::string key_str() const override { return key_name; }

    std::string value_str() const override { return std::to_string(value); }

    void from_string(const std::string &s) override {
      std::istringstream iss(s);
      iss >> value;
    }

	std::string key_name;
    T value;
  };

public:
  EXPORT Settings(const std::string&storage_path);
  EXPORT ~Settings();

  EXPORT void set_default();

  EXPORT void save();
  EXPORT void save(const std::string &file);
  EXPORT void load(const std::string &file);
  EXPORT std::vector<utils::async::ThreadPool::Params> thread_pools_params();

  EXPORT std::string dump();
  EXPORT void change(std::string& expression);
  // aof level options;
  std::string path;

  Option<uint64_t> aof_max_size;  // measurements count in one file
  Option<uint64_t> aof_buffer_size; // inner buffer size

  Option<uint32_t> chunk_size;

  Option<STRATEGY> strategy;

  // memstorage options;
  Option<uint32_t> memory_limit; //in bytes;
  Option<float>  percent_when_start_droping; //fill percent, when start dropping.
  Option<float>  percent_to_drop; //how many chunk drop.
  
  bool  load_min_max; //if true - engine dont load min max. needed to ctl tool.
};

using Settings_ptr = std::shared_ptr<Settings>;
#ifndef MSVC
template<>
EXPORT std::string Settings::Option<STRATEGY>::value_str()const;
#else
template<>
std::string Settings::Option<STRATEGY>::value_str()const {
	std::stringstream ss;
	ss << this->value;
	return ss.str();
}
#endif
}
}
