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

protected:
  std::string _filename;
};
}
}
