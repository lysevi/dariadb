#pragma once

#include "utils.h"
#include <list>
#include <memory>
#include <string>

namespace dariadb {
namespace utils {
namespace fs {
std::list<std::string> ls(const std::string &path);
std::list<std::string> ls(const std::string &path, const std::string &ext);

bool rm(const std::string &rm_path);

std::string filename(const std::string &fname); // without ex
std::string random_file_name(const std::string ext);

std::string parent_path(const std::string &fname);
std::string append_path(const std::string &p1, const std::string &p2);

bool path_exists(const std::string &path);
void mkdir(const std::string &path);

/// ext - ".ext"

class MappedFile : public utils::NonCopy {
  class Private;
  MappedFile(Private *im);

public:
  using MapperFile_ptr = std::shared_ptr<MappedFile>;

  ~MappedFile();
  void close();
  uint8_t *data();

  void flush(std::size_t offset = 0, std::size_t bytes = 0);

  static MapperFile_ptr open(const std::string &path, bool read_only=false);
  static MapperFile_ptr touch(const std::string &path, uint64_t size);

private:
  std::unique_ptr<Private> _impl;
};
}
}
}
