#include <libdariadb/timeutil.h>
#include <libdariadb/utils/exception.h>
#include <libdariadb/utils/fs.h>
#include <boost/filesystem.hpp>
#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <fstream>
#include <iterator>

#include <boost/date_time/posix_time/posix_time.hpp>

void empty_function_fs_win32() {}

#ifdef WIN32
#include <windows.h>
#pragma comment(lib, "advapi32.lib")
namespace {
	std::string errocode_to_string(DWORD errorCode)
	{
		if (errorCode != 0)
		{

			LPSTR messageBuffer = nullptr;
			size_t size = FormatMessageA(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
				NULL, errorCode, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), (LPSTR)&messageBuffer, 0, NULL);

			std::string message(messageBuffer, size);

			LocalFree(messageBuffer);
			return message;
		}
		return std::string();
	}

	BOOL WinApiSetPrivilege()
	{
		//LPCTSTR lpszPrivilege = "SeCreateGlobalPrivilege";
		//BOOL bEnablePrivilege = TRUE;
		//HANDLE hToken;
		//// Open a handle to the access token for the calling process. That is this running program
		//if (!OpenProcessToken(GetCurrentProcess(), TOKEN_ADJUST_PRIVILEGES, &hToken))
		//{
		//	auto ec = GetLastError();
		//	MAKE_EXCEPTION("OpenProcessToken() error:"+ errocode_to_string(ec));
		//	return FALSE;
		//}

		//TOKEN_PRIVILEGES tp;
		//LUID luid;

		//if (!LookupPrivilegeValue(
		//	NULL,            // lookup privilege on local system
		//	lpszPrivilege,   // privilege to lookup 
		//	&luid))        // receives LUID of privilege
		//{
		//	printf("LookupPrivilegeValue error: %u\n", GetLastError());
		//	return FALSE;
		//}

		//tp.PrivilegeCount = 1;
		//tp.Privileges[0].Luid = luid;
		//if (bEnablePrivilege)
		//	tp.Privileges[0].Attributes = SE_PRIVILEGE_ENABLED;
		//else
		//	tp.Privileges[0].Attributes = 0;

		//// Enable the privilege or disable all privileges.

		//if (!AdjustTokenPrivileges(
		//	hToken,
		//	FALSE,
		//	&tp,
		//	sizeof(TOKEN_PRIVILEGES),
		//	(PTOKEN_PRIVILEGES)NULL,
		//	(PDWORD)NULL))
		//{
		//	throw MAKE_EXCEPTION("AdjustTokenPrivileges error:"+GetLastError());
		//}

		//if (GetLastError() == ERROR_NOT_ALL_ASSIGNED){
		//	throw MAKE_EXCEPTION("WinApiSetPrivilege " + errocode_to_string(GetLastError()));
		//}

		return TRUE;
	}
}

namespace dariadb {
namespace utils {
namespace fs {

class MappedFile::Private {
public:

  Private() {

    _closed = false;
	dataPtr = NULL;
	hMapping = NULL;
	hFile = NULL;

  }

  ~Private() {
    if (!_closed) {
      close();
    }
  }

  void resize(std::size_t newSize, bool updateDataPtr = true) {

	  if (dataPtr != NULL) {
		  UnmapViewOfFile(dataPtr);
	  }
	  if (hMapping != NULL) {
		  CloseHandle(hMapping);
	  }

	  auto result = SetFilePointer(hFile, static_cast<DWORD>(newSize), NULL, FILE_BEGIN);
	  if (result != NULL)
		  result = SetEndOfFile(hFile);
	  /*if(result!=NULL)
		result = SetFilePointer(hFile, 0, NULL, FILE_BEGIN);*/
	  if (result == NULL) {
		  auto errorCode = GetLastError();
		  std::stringstream ss;
		  ss << "result error: code:" << errorCode;
		  if (errorCode != 0){
			  ss << errocode_to_string(errorCode);
		  }
		  auto msg = ss.str();
		  throw MAKE_EXCEPTION(msg);
	  }
	  if (updateDataPtr) {
		  remapFile();
	  }

  }

  void remapFile() {
	  hMapping = CreateFileMapping(hFile, nullptr, _read_only ? PAGE_READONLY : PAGE_READWRITE, 0, 0, nullptr);
	  if (hMapping == nullptr) {
		  throw MAKE_EXCEPTION("remapFile - CreateFileMapping failed, fname = " + m_path);
	  }

	  auto dwFileSize = size();
	  dataPtr = (unsigned char*)MapViewOfFile(hMapping,
		  _read_only ? FILE_MAP_READ : FILE_MAP_ALL_ACCESS,
		  0,
		  0,
		  dwFileSize);
	  if (dataPtr == nullptr) {
		  auto msg = errocode_to_string(GetLastError());
		  throw MAKE_EXCEPTION("remapFile - MapViewOfFile failed msg:" + msg);
	  }
  }
	

  std::size_t size() {
    // TODO Cache it!
    DWORD dwFileSize = GetFileSize(hFile, nullptr);
    if (dwFileSize == INVALID_FILE_SIZE) {
      CloseHandle(hFile);
	  throw  MAKE_EXCEPTION("fileMappingCreate - GetFileSize failed, fname = " + m_path);
    }
    return static_cast<std::size_t>(dwFileSize);
  }

  static Private *open(const std::string &path, bool read_only, std::size_t with_size=0) {
	  auto result = new Private();
	  result->m_path = path;
	  result->_read_only = read_only;
	  auto open_flag = GENERIC_READ;
	  if (!read_only) {
		  open_flag |= GENERIC_WRITE;
	  }
	  try
	  {
		  WinApiSetPrivilege();
		  result->hFile = CreateFile(path.c_str(), open_flag, 0, nullptr, OPEN_ALWAYS,
			  FILE_ATTRIBUTE_NORMAL, nullptr);
		  if (result->hFile == INVALID_HANDLE_VALUE) {
			  throw MAKE_EXCEPTION("fileMappingCreate - CreateFile failed, fname = " + path);
		  }
		  if (with_size!=0) {
			  result->resize(with_size, false);
		  }
		  result->remapFile();
	  }
	  catch (std::exception &ex)
	  {
		  auto errorCode = GetLastError();
		  std::stringstream ss;
		  ss << "Exception: " << ex.what();
		  ss << ", code:" << errorCode;
		  if (errorCode != 0)
		  {
			  ss << errocode_to_string(errorCode);
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

  void close() {
    _closed = true;
	UnmapViewOfFile(dataPtr);
    CloseHandle(hMapping);
    CloseHandle(hFile);
  }

  uint8_t *data() {
    return dataPtr;
  }

  void flush(std::size_t offset = 0, std::size_t bytes = 0) {
	  auto result=FlushViewOfFile(dataPtr+offset, bytes);
	  if (result != TRUE)
	  {
		  auto errorCode = GetLastError();
		  std::stringstream ss;
		  ss << "flush error. code:" << errorCode;
		  if (errorCode != 0)
		  {
			  ss << errocode_to_string(errorCode);
		  }
		  auto msg = ss.str();
		  throw MAKE_EXCEPTION(msg);
	  }
  }

protected:
  bool _closed;
  bool _read_only;
  std::size_t fsize;
  HANDLE hFile;
  HANDLE hMapping;

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
  _impl->close();
}

uint8_t *MappedFile::data() {
  return _impl->data();
}

void MappedFile::flush(std::size_t offset, std::size_t bytes) {
  _impl->flush(offset, bytes);
}

} // namespace fs
} // namespace utils
} // namespace dariadb

#endif