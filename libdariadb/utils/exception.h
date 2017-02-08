#pragma once
#include <libdariadb/utils/logger.h>
#include <libdariadb/utils/strings.h>
#include <stdexcept>
#include <string>

#ifdef UNIX_OS
#include <execinfo.h>
#include <sstream>
#include <stdlib.h>
#include <unistd.h>
#define BT_BUF_SIZE 512
#endif

#define CODE_POS (dariadb::utils::CodePos(__FILE__, __LINE__, __FUNCTION__))

#define MAKE_EXCEPTION(msg)                                                    \
  dariadb::utils::Exception::create_and_log(CODE_POS, msg)
// macros, because need CODE_POS

#ifdef DEBUG
#define THROW_EXCEPTION(...)                                                   \
  dariadb::utils::Exception::create_and_log(CODE_POS, __VA_ARGS__);            \
  std::exit(1);
#else
#define THROW_EXCEPTION(...)                                                   \
  throw dariadb::utils::Exception::create_and_log(CODE_POS, __VA_ARGS__);
#endif

namespace dariadb {
namespace utils {

struct CodePos {
  const char *_file;
  const int _line;
  const char *_func;

  CodePos(const char *file, int line, const char *function)
      : _file(file), _line(line), _func(function) {}

  std::string toString() const {
    auto ss = std::string(_file) + " line: " + std::to_string(_line) +
              " function: " + std::string(_func) + "\n";
    return ss;
  }
  CodePos &operator=(const CodePos &) = delete;
};

class Exception : public std::exception {
public:
  template <typename... T>
  static Exception create_and_log(const CodePos &pos, T... message) {

    auto expanded_message = utils::strings::args_to_string(message...);
    auto str_message =
        std::string("FATAL ERROR. The Exception will be thrown! ") +
        pos.toString() + " Message: " + expanded_message;

#ifdef UNIX_OS
    std::stringstream sstr;
    sstr << str_message;
    int j, nptrs;
    void *buffer[BT_BUF_SIZE];
    char **strings;

    nptrs = backtrace(buffer, BT_BUF_SIZE);
    sstr << "\nbacktrace() returned " << nptrs << " addresses" << std::endl;

    /* The call backtrace_symbols_fd(buffer, nptrs, STDOUT_FILENO)
    would produce similar output to the following: */

    strings = backtrace_symbols(buffer, nptrs);
    if (strings == NULL) {
      sstr << "backtrace_symbols" << std::endl;
    } else {
      sstr << "trace:" << std::endl;
      for (j = 0; j < nptrs; j++) {
        sstr << strings[j] << std::endl;
      }
    }
    free(strings);
    str_message = sstr.str();
#endif

    logger_fatal(str_message);
    return Exception(expanded_message);
  }

public:
  virtual const char *what() const noexcept { return _msg.c_str(); }
  const std::string &message() const { return _msg; }

protected:
  Exception() {}
  Exception(const char *&message) : _msg(std::string(message)) {}
  Exception(const std::string &message) : _msg(message) {}

private:
  std::string _msg;
};
}
}
