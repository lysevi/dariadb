#pragma once
#include "libdariadb/utils/logger.h"
#include "libdariadb/utils/strings.h"
#include <sstream>
#include <stdexcept>
#include <string>

#define CODE_POS (dariadb::utils::CodePos(__FILE__, __LINE__, __FUNCTION__))

#define MAKE_EXCEPTION(msg)                                                    \
  dariadb::utils::Exception::create_and_log(CODE_POS, msg)
// macros, because need CODE_POS
#define THROW_EXCEPTION(...)                                                \
  throw dariadb::utils::Exception::create_and_log(CODE_POS, __VA_ARGS__);

namespace dariadb {
namespace utils {

struct CodePos {
  const char *_file;
  const int _line;
  const char *_func;

  CodePos(const char *file, int line, const char *function)
      : _file(file), _line(line), _func(function) {}

  std::string toString() const {
    std::stringstream ss;
    ss << _file << " line: " << _line << " function: " << _func << std::endl;
    return ss.str();
  }
  CodePos &operator=(const CodePos &) = delete;
};

class Exception : public std::exception {
public:
  template <typename... T>
  static Exception create_and_log(const CodePos &pos, T... message) {
    std::stringstream ss;
    auto expanded_message = utils::strings::args_to_string(message...);
    ss << "FATAL ERROR. The Exception will be thrown! " << pos.toString()
       << " Message: " << expanded_message;
    logger_fatal(ss.str());
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
