#pragma once

#include <libdariadb/utils/locker.h>
#include <libdariadb/st_exports.h>
#include <list>
#include <memory>
#include <string>

namespace dariadb {
namespace storage {

const std::string MANIFEST_FILE_NAME = "Manifest";

class Manifest {
  Manifest(const std::string &fname);

public:
  EXPORT Manifest() = delete;
  EXPORT static void start(const std::string &fname);
  EXPORT static void stop();
  EXPORT static Manifest *instance();
  EXPORT void restore();

  EXPORT std::list<std::string> page_list();
  EXPORT void page_append(const std::string &rec);
  EXPORT void page_rm(const std::string &rec);

  EXPORT std::list<std::string> cola_list();
  EXPORT void cola_append(const std::string &rec);
  EXPORT void cola_rm(const std::string &rec);

  EXPORT std::list<std::string> aof_list();
  EXPORT void aof_append(const std::string &rec);
  EXPORT void aof_rm(const std::string &rec);

  EXPORT std::string read_file(const std::string &fname);
  EXPORT void write_file(const std::string &fname, const std::string &content);

  EXPORT void set_version(const std::string &version);
  EXPORT std::string get_version();

private:
  void touch();

  void clear_field_values(std::string field_name);

protected:
  std::string _filename;
  static std::unique_ptr<Manifest> _instance;
  utils::Locker _locker;
};
}
}
