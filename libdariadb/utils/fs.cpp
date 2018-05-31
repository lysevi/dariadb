#include <libdariadb/timeutil.h>
#include <libdariadb/utils/exception.h>
#include <libdariadb/utils/fs.h>
#include <boost/filesystem.hpp>
#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <fstream>
#include <iterator>

#include <boost/date_time/posix_time/posix_time.hpp>

using namespace dariadb::utils::fs;
namespace bi = boost::interprocess;

namespace dariadb {
namespace utils {
namespace fs {
std::list<std::string> ls(const std::string &path) {

  std::list<std::string> s_result;
  if (!path_exists(path)) {
    return s_result;
  }
  std::list<boost::filesystem::path> result;
  std::copy(boost::filesystem::directory_iterator(path),
            boost::filesystem::directory_iterator(), std::back_inserter(result));

  for (boost::filesystem::path &it : result) {
    s_result.push_back(it.string());
  }
  return s_result;
}

std::list<std::string> ls(const std::string &path, const std::string &ext) {
  std::list<std::string> s_result;
  if (!path_exists(path)) {
    return s_result;
  }

  std::list<boost::filesystem::path> result;
  std::copy(boost::filesystem::directory_iterator(path),
            boost::filesystem::directory_iterator(), std::back_inserter(result));

  // ext filter
  result.remove_if(
      [&ext](boost::filesystem::path p) { return p.extension().string() != ext; });

  for (boost::filesystem::path &it : result) {
    s_result.push_back(it.string());
  }
  return s_result;
}

bool rm(const std::string &rm_path) {
  if (!boost::filesystem::exists(rm_path))
    return true;
  try {
    if (boost::filesystem::is_directory(rm_path)) {
      boost::filesystem::path path_to_remove(rm_path);
      for (boost::filesystem::directory_iterator end_dir_it, it(path_to_remove);
           it != end_dir_it; ++it) {
        if (!boost::filesystem::remove_all(it->path())) {
          return false;
        }
      }
    }
    boost::filesystem::remove_all(rm_path);
    return true;
  } catch (boost::filesystem::filesystem_error &ex) {
    std::string msg = ex.what();
    MAKE_EXCEPTION("utils::rm exception: " + msg);
    throw;
  }
}

std::string filename(const std::string &fname) { // without ex
  boost::filesystem::path p(fname);
  return p.stem().string();
}

std::string extract_filename(const std::string &fname) {
  boost::filesystem::path p(fname);
  return p.filename().string();
}

std::string random_file_name(const std::string &ext) {
  auto now = boost::posix_time::microsec_clock::local_time();
  auto duration = now - boost::posix_time::from_time_t(0);
  std::stringstream ss;
  ss << duration.total_microseconds() << ext;
  return ss.str();
}

std::string parent_path(const std::string &fname) {
  boost::filesystem::path p(fname);
  return p.parent_path().string();
}

std::string append_path(const std::string &p1, const std::string &p2) {
  boost::filesystem::path p(p1);
  boost::filesystem::path p_sub(p2);
  p /= p_sub;
  return p.string();
}

bool file_exists(const std::string &fname) {
  std::ifstream fs(fname.c_str());
  if (fs) {
    return true;
  } else {
    return false;
  }
}

bool path_exists(const std::string &path) {
  return boost::filesystem::exists(path);
}

void mkdir(const std::string &path) {
  if (!boost::filesystem::exists(path)) {
    boost::filesystem::create_directory(path);
  }
}

std::string read_file(const std::string &fname) {
  std::ifstream fs;
  fs.open(fname);
  if (!fs.is_open()) {
    throw std::runtime_error("(!fs.is_open())");
  }

  std::stringstream ss;
  std::string line;
  while (std::getline(fs, line)) {
    ss << line;
  }
  fs.close();
  return ss.str();
}
} // namespace fs
} // namespace utils
} // namespace dariadb