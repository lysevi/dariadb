#include <libdariadb/utils/fs.h>
#include <libdariadb/timeutil.h>
#include <libdariadb/utils/exception.h>
#include <fstream>
#include <iterator>
#include <sstream>

#ifndef WIN32
#include <string.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace dariadb {
namespace utils {
namespace fs {

class MappedFile::Private {
public:

  Private() {
	  fd = 0;
	  dataPtr = nullptr;
  }

  ~Private() {
    if (!_closed) {
		closeMap();
    }
  }

  void resize(std::size_t newSize, bool updateDataPtr = true) {
	  auto sz = size();
	  if (dataPtr != nullptr) {
		  munmap(dataPtr, sz);
	  }
	  auto result = truncate(m_path.c_str(), newSize);
	  if (result != 0) {
		  auto errorCode = errno;
		  std::stringstream ss;
		  ss << "result error: code:" << errorCode;
		  if (errorCode != 0)
		  {
			  ss << strerror(errno);
		  }
		  auto msg = ss.str();
		  throw MAKE_EXCEPTION(msg);
	  }
	  if (updateDataPtr) {
		  remapFile();
	  }

  }

  void remapFile() {
	  auto fsize = size();
	  dataPtr = (unsigned char*)mmap(nullptr, fsize, _read_only?PROT_READ: PROT_WRITE, MAP_PRIVATE, fd, 0);
	  if (dataPtr == MAP_FAILED) {
		  std::stringstream ss;
		  ss<< "fileMappingCreate - mmap failed, fname = "
			  << m_path << ", " << strerror(errno) << std::endl;
		  throw MAKE_EXCEPTION(ss.str());
	  }
  }
	

  std::size_t size() {
    // TODO Cache it!
	  struct stat st;
	  if (fstat(fd, &st) < 0) {
		  std::stringstream ss;
		  ss<< "fileMappingCreate - fstat failed, fname = "
			  << m_path << ", " << strerror(errno);
		  throw MAKE_EXCEPTION(ss.str());
	  }
	  return (size_t)st.st_size;
  }

  static Private *open(const std::string &path, bool read_only, std::size_t with_size=0) {
	  auto result = new Private();
	  result->m_path = path;
	  result->_read_only = read_only;
	  auto open_flag = O_RDONLY;
	  if (!read_only) {
		  open_flag = O_RDWR ;
	  }
	  
	  try
	  {
		  result->fd = ::open(result->m_path.c_str(), O_RDONLY, 0);
		  if (result->fd < 0) {
			  throw MAKE_EXCEPTION("fileMappingCreate - CreateFile failed, fname = " + path +" " + strerror(errno));
		  }
		  if (with_size!=0) {
			  result->resize(with_size, false);
		  }
		  result->remapFile();
	  }
	  catch (std::exception &ex)
	  {
		  std::stringstream ss;
		  ss << "Exception: " << ex.what();
		  ss << ", code:" << errno;
		  if (errno != 0)
		  {
			  ss << strerror(errno);
		  }
		  auto msg = ss.str();
		  throw MAKE_EXCEPTION(msg);
	  }
    return result;
  }

  static Private *touch(const std::string &path, uint64_t size) {
		auto result = Private::open(path, false, size);
		return result;
  }

  void closeMap() {
    _closed = true;
	auto sz = size();
	munmap(dataPtr, sz);
	::close(fd);
  }

  uint8_t *data() {
    return dataPtr;
  }

  void flush(std::size_t offset = 0, std::size_t bytes = 0) {
	  
	  auto result= msync(dataPtr + offset, bytes, MS_SYNC);
	  if (result != 0)
	  {
		  auto errorCode = errno;
		  std::stringstream ss;
		  ss << "flush error. code:" << errorCode;
		  if (errorCode != 0){
			  ss << strerror(errno);
		  }
		  auto msg = ss.str();
		  throw MAKE_EXCEPTION(msg);
	  }
  }

protected:
  bool _closed;
  bool _read_only;
  int fd;
  uint8_t *dataPtr;
  std::string m_path;
};

MappedFile::MappedFile(Private *im) : _impl(im) {}

MappedFile::~MappedFile() {}

void MappedFile::resize(std::size_t newSize) {
  return _impl->resize(newSize);
}

std::size_t MappedFile::size() {
  return _impl->size();
}

MappedFile::MapperFile_ptr MappedFile::open(const std::string &path, bool read_only) {
  auto impl_res = MappedFile::Private::open(path, read_only);
  return std::make_shared<MappedFile>(impl_res);
}

MappedFile::MapperFile_ptr MappedFile::touch(const std::string &path, uint64_t size) {
  auto impl_res = MappedFile::Private::touch(path, size);
  return std::make_shared<MappedFile>(impl_res);
}

void MappedFile::close() {
  _impl->closeMap();
}

uint8_t *MappedFile::data() {
  return _impl->data();
}

void MappedFile::flush(std::size_t offset, std::size_t bytes) {
  _impl->flush(offset, bytes);
}
#endif
} // namespace fs
} // namespace utils
} // namespace dariadb