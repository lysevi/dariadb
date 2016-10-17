#pragma once

#include <libdariadb/utils/utils.h>
#include <libdariadb/dariadb_st_exports.h>
#include <list>
#include <memory>
#include <string>

namespace dariadb {
namespace utils {
namespace fs {
DARIADB_ST_EXPORTS std::list<std::string> ls(const std::string &path);
DARIADB_ST_EXPORTS std::list<std::string> ls(const std::string &path, const std::string &ext);

DARIADB_ST_EXPORTS bool rm(const std::string &rm_path);

DARIADB_ST_EXPORTS std::string filename(const std::string &fname); // without ex
DARIADB_ST_EXPORTS std::string extract_filename(const std::string &fname);
DARIADB_ST_EXPORTS std::string random_file_name(const std::string ext);

DARIADB_ST_EXPORTS std::string parent_path(const std::string &fname);
DARIADB_ST_EXPORTS std::string append_path(const std::string &p1, const std::string &p2);

DARIADB_ST_EXPORTS bool path_exists(const std::string &path);
DARIADB_ST_EXPORTS void mkdir(const std::string &path);

DARIADB_ST_EXPORTS std::string read_file(const std::string &fname);
/// ext - ".ext"

class MappedFile : public utils::NonCopy {
  class Private;
  MappedFile(Private *im);

public:
  using MapperFile_ptr = std::shared_ptr<MappedFile>;

  DARIADB_ST_EXPORTS ~MappedFile();
  DARIADB_ST_EXPORTS void close();
  DARIADB_ST_EXPORTS uint8_t *data();

  DARIADB_ST_EXPORTS void flush(std::size_t offset = 0, std::size_t bytes = 0);

  DARIADB_ST_EXPORTS static MapperFile_ptr open(const std::string &path, bool read_only = false);
  DARIADB_ST_EXPORTS static MapperFile_ptr touch(const std::string &path, uint64_t size);

private:
  std::unique_ptr<Private> _impl;
};
}
}
}
