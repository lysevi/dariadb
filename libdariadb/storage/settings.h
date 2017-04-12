#pragma once

#include <libdariadb/engines/strategy.h>
#include <libdariadb/meas.h>
#include <libdariadb/st_exports.h>
#include <libdariadb/utils/async/thread_pool.h>
#include <libdariadb/utils/logger.h>

#include <string>
#include <unordered_map>

namespace dariadb {
namespace storage {

const std::string SETTINGS_FILE_NAME = "Settings";
const std::string MEMORY_ONLY_PATH = "::memory_only::";

class BaseOption {
public:
  EXPORT virtual ~BaseOption();
  /// return key in option file.
  virtual std::string key_str() const = 0;
  /// convert option value to string
  virtual std::string value_str() const = 0;
  /// read from string :)
  virtual void from_string(const std::string &s) = 0;
};
class Settings;
using Settings_ptr = std::shared_ptr<Settings>;
/**
settings - is a dictionary on disk.
*/
class Settings {
  std::unordered_map<std::string, BaseOption *> _all_options;

  template <typename T> class ReadOnlyOption : public BaseOption {
  public:
    ReadOnlyOption() = delete;
    ReadOnlyOption(Settings *s, const std::string &keyname, const T default_value)
        : key_name(keyname), _value(default_value) {

      if (s != nullptr) { /// register option in settings dict.
        auto sres = s->_all_options.find(keyname);
        if (sres == s->_all_options.end()) {
          s->_all_options.emplace(keyname, this);
        } else {
          THROW_EXCEPTION("Option duplicate key.");
        }
      }
    }

    std::string key_str() const override { return key_name; }
    std::string value_str() const override { return std::to_string(_value); }
    void from_string(const std::string &s) override;
    /// return option value
    T value() const { return _value; }

  protected:
    std::string key_name;
    T _value;
  };

  template <typename T> class Option : public ReadOnlyOption<T> {
  public:
    Option() = delete;
    Option(Settings *s, const std::string &keyname, const T default_value)
        : ReadOnlyOption<T>(s, keyname, default_value) {}

    void setValue(const T &value_) {
      logger_info("engine: change settings - ", this->key_name, " ", this->_value, " to ",
                  value_);
      this->_value = value_;
    }
  };

public:
  EXPORT static Settings_ptr create(const std::string &storage_path);
  /**
  For memory only storage.
  */
  EXPORT static Settings_ptr create();
  EXPORT ~Settings();

  EXPORT void set_default();

  EXPORT void save();
  EXPORT void save(const std::string &file);
  EXPORT void load(const std::string &file);
  EXPORT std::vector<utils::async::ThreadPool::Params> thread_pools_params();

  EXPORT std::string dump();
  EXPORT void change(std::string &expression);

  ReadOnlyOption<std::string> storage_path;
  ReadOnlyOption<std::string> raw_path;

  Option<Time> max_store_period; // period, when old pages must be removed.
  // wal level options;
  Option<uint64_t> wal_file_size;  // measurements count in one file
  Option<uint64_t> wal_cache_size; // inner buffer size

  Option<uint64_t> max_chunks_per_page; // work when drop from memstorage to pages.
  Option<uint32_t> chunk_size;

  Option<STRATEGY> strategy;

  // memstorage options;
  Option<uint32_t> memory_limit;            // in bytes;
  Option<float> percent_when_start_droping; // fill percent, when start dropping.
  Option<float> percent_to_drop;            // how many chunk drop.
  // pages per level.
  Option<uint16_t> max_pages_in_level;

  Option<size_t> threads_in_common; // threads count in pool 'COMMON'
  Option<size_t> threads_in_diskio; // threads count in pool 'DISK_IO'

  bool load_min_max; // if true - engine dont load min max. needed to ctl tool.
  std::string alias; // is set, used in log messages;
  bool is_memory_only_mode;

protected:
  EXPORT Settings(const std::string &storage_path);
};

template <> EXPORT std::string Settings::ReadOnlyOption<STRATEGY>::value_str() const;
template <> EXPORT std::string Settings::ReadOnlyOption<std::string>::value_str() const;
}
}
