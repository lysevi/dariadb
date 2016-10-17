#pragma once

#include <libdariadb/utils/locker.h>
#include <libdariadb/dariadb_st_exports.h>
#include <list>
#include <memory>
#include <string>

namespace dariadb {
namespace storage {

const std::string MANIFEST_FILE_NAME = "Manifest";

class Manifest {
  Manifest(const std::string &fname);

public:
  DARIADB_ST_EXPORTS Manifest() = delete;
  DARIADB_ST_EXPORTS static void start(const std::string &fname);
  DARIADB_ST_EXPORTS static void stop();
  DARIADB_ST_EXPORTS static Manifest *instance();
  DARIADB_ST_EXPORTS void restore();

  DARIADB_ST_EXPORTS std::list<std::string> page_list();
  DARIADB_ST_EXPORTS void page_append(const std::string &rec);
  DARIADB_ST_EXPORTS void page_rm(const std::string &rec);

  DARIADB_ST_EXPORTS std::list<std::string> cola_list();
  DARIADB_ST_EXPORTS void cola_append(const std::string &rec);
  DARIADB_ST_EXPORTS void cola_rm(const std::string &rec);

  DARIADB_ST_EXPORTS std::list<std::string> aof_list();
  DARIADB_ST_EXPORTS void aof_append(const std::string &rec);
  DARIADB_ST_EXPORTS void aof_rm(const std::string &rec);

  DARIADB_ST_EXPORTS std::string read_file(const std::string &fname);
  DARIADB_ST_EXPORTS void write_file(const std::string &fname, const std::string &content);

  DARIADB_ST_EXPORTS void set_version(const std::string &version);
  DARIADB_ST_EXPORTS std::string get_version();

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
