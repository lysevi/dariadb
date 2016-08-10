#pragma once
#include "logger.h"
#include <sstream>
#include <stdexcept>
#include <string>

#define CODE_POS (dariadb::utils::CodePos(__FILE__, __LINE__, __FUNCTION__))

#define MAKE_EXCEPTION(msg) dariadb::utils::Exception::create_and_log(CODE_POS, msg)
#define THROW_EXCEPTION_SS(msg)                                                          \
  std::stringstream sstream_var;                                                         \
  sstream_var << msg;                                                                    \
  MAKE_EXCEPTION(sstream_var.str());

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
  static Exception create_and_log(const CodePos &pos, const std::string &message) {
    logger_fatal("FATAL ERROR. The Exception will be thrown! "
                 << pos.toString() << " Message: " << message);
    return Exception(message);
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
