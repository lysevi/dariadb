#pragma once

#include <list>
#include <string>

namespace dariadb {
namespace storage {
class Manifest {
public:
  Manifest(const std::string &fname);
  std::list<std::string> page_list();
  void page_append(const std::string &rec);

  std::string read_file(const std::string &fname);
  void write_file(const std::string &fname,const std::string &content);
private:
  void touch();
protected:
  std::string _filename;
};
}
}
