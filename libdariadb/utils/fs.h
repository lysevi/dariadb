#pragma once

#include <libdariadb/st_exports.h>
#include <libdariadb/utils/utils.h>
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
EXPORT bool file_exists(const std::string &fname);

EXPORT void mkdir(const std::string &path);

EXPORT std::string read_file(const std::string &fname);
}
}
}
