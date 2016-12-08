#include <libdariadb/utils/fs.h>
#include <libdariadb/utils/exception.h>
#include <boost/filesystem.hpp>
#include <fstream>
#include <iterator>

#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>

#include <boost/uuid/uuid.hpp>            // uuid class
#include <boost/uuid/uuid_generators.hpp> // generators
#include <boost/uuid/uuid_io.hpp>         // streaming operators etc.

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

std::string random_file_name(const std::string&ext) {
  boost::uuids::uuid uuid = boost::uuids::random_generator()();
  std::stringstream ss;
  ss << uuid << ext;
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
	}
	else {
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
}
}
}

class MappedFile::Private {
public:
  Private() {
    _closed = false;
    m_file = nullptr;
    m_region = nullptr;
  }

  ~Private() {
    if (!_closed) {
      close();
    }
  }

  static Private *open(const std::string &path, bool read_only) {
    auto result = new Private();
    auto open_flag = read_only ? bi::read_only : bi::read_write;
    result->m_file =
        std::unique_ptr<bi::file_mapping>{new bi::file_mapping(path.c_str(), open_flag)};
    result->m_region = std::unique_ptr<bi::mapped_region>{
        new bi::mapped_region(*(result->m_file), open_flag)};
    return result;
  }

  static Private *touch(const std::string &path, uint64_t size) {
    try {
      bi::file_mapping::remove(path.c_str());
      std::filebuf fbuf;
      fbuf.open(path, std::ios_base::in | std::ios_base::out | std::ios_base::trunc |
                          std::ios_base::binary);
      // Set the size
      fbuf.pubseekoff(size - 1, std::ios_base::beg);
      fbuf.sputc(0);
      fbuf.close();
      return Private::open(path, false);
    } catch (std::runtime_error &ex) {
      std::string what = ex.what();
      throw MAKE_EXCEPTION(what);
    }
  }

  void close() {
    _closed = true;
    m_region->flush();
    m_region = nullptr;
    m_file = nullptr;
  }

  uint8_t *data() { return static_cast<uint8_t *>(m_region->get_address()); }

  void flush(std::size_t offset = 0, std::size_t bytes = 0) {
#ifdef DEBUG
    // in debug sync flush is very slow.
    auto flush_res = this->m_region->flush(offset, bytes, true);
    assert(flush_res);
#else
    this->m_region->flush(offset, bytes, true);
#endif
  }

protected:
  bool _closed;

  std::unique_ptr<bi::file_mapping> m_file;
  std::unique_ptr<bi::mapped_region> m_region;
};

MappedFile::MappedFile(Private *im) : _impl(im) {}

MappedFile::~MappedFile() {}

MappedFile::MapperFile_ptr MappedFile::open(const std::string &path, bool read_only) {
  auto impl_res = MappedFile::Private::open(path, read_only);
  MappedFile::MapperFile_ptr result{new MappedFile{impl_res}};
  return result;
}

MappedFile::MapperFile_ptr MappedFile::touch(const std::string &path, uint64_t size) {
  auto impl_res = MappedFile::Private::touch(path, size);
  MappedFile::MapperFile_ptr result{new MappedFile{impl_res}};
  return result;
}

void MappedFile::close() {
  _impl->close();
}

uint8_t *MappedFile::data() {
  return _impl->data();
}

void MappedFile::flush(std::size_t offset, std::size_t bytes) {
  _impl->flush(offset, bytes);
}
