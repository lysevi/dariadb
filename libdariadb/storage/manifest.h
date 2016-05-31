#pragma once

#include <list>
#include <string>
#include <memory>

namespace dariadb {
namespace storage {
class Manifest {
	Manifest(const std::string &fname);
public:
	Manifest() = delete;
	static void start(const std::string &fname);
	static void stop();
	static Manifest*instance();
  std::list<std::string> page_list();
  void page_append(const std::string &rec);

  std::string read_file(const std::string &fname);
  void write_file(const std::string &fname,const std::string &content);
private:
  void touch();
protected:
  std::string _filename;
  static std::unique_ptr<Manifest> _instance;
};
}
}
