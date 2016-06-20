#pragma once

#include "../utils/locker.h"
#include <list>
#include <memory>
#include <string>

namespace dariadb {
namespace storage {
class Manifest {
  Manifest(const std::string &fname);

public:
  Manifest() = delete;
  static void start(const std::string &fname);
  static void stop();
  static Manifest *instance();

  std::list<std::string> page_list();
  void page_append(const std::string &rec);

  std::list<std::string> cola_list();
  void cola_append(const std::string &rec);
  void cola_rm(const std::string &rec);

  std::list<std::string> aof_list();
  void aof_append(const std::string &rec);
  void aof_rm(const std::string &rec);

  std::string read_file(const std::string &fname);
  void write_file(const std::string &fname, const std::string &content);

private:
  void touch();

protected:
  std::string _filename;
  static std::unique_ptr<Manifest> _instance;
  utils::Locker _locker;
};
}
}
