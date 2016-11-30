#pragma once

#include <libdariadb/utils/utils.h>
#include <libdariadb/st_exports.h>
#include <list>
#include <memory>
#include <string>

namespace dariadb {
namespace utils {
namespace fs {
EXPORT std::list<std::string> ls(const std::string &path);
EXPORT std::list<std::string> ls(const std::string &path, const std::string &ext);

EXPORT bool rm(const std::string &rm_path);

EXPORT std::string filename(const std::string &fname); // without ex
EXPORT std::string extract_filename(const std::string &fname);
EXPORT std::string random_file_name(const std::string &ext);

EXPORT std::string parent_path(const std::string &fname);
EXPORT std::string append_path(const std::string &p1, const std::string &p2);

EXPORT bool path_exists(const std::string &path);
EXPORT void mkdir(const std::string &path);

EXPORT std::string read_file(const std::string &fname);
/// ext - ".ext"

class MappedFile : public utils::NonCopy {
  class Private;
  MappedFile(Private *im);

public:
  using MapperFile_ptr = std::shared_ptr<MappedFile>;

  EXPORT ~MappedFile();
  EXPORT void close();
  EXPORT uint8_t *data();

  EXPORT void flush(std::size_t offset = 0, std::size_t bytes = 0);

  EXPORT static MapperFile_ptr open(const std::string &path, bool read_only = false);
  EXPORT static MapperFile_ptr touch(const std::string &path, uint64_t size);

private:
  std::unique_ptr<Private> _impl;
};
}
}
}
