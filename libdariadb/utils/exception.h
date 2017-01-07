#pragma once
#include <libdariadb/utils/logger.h>
#include <libdariadb/utils/strings.h>
#include <stdexcept>
#include <string>

#define CODE_POS (dariadb::utils::CodePos(__FILE__, __LINE__, __FUNCTION__))

#define MAKE_EXCEPTION(msg) dariadb::utils::Exception::create_and_log(CODE_POS, msg)
// macros, because need CODE_POS

#ifdef DEBUG
#define THROW_EXCEPTION(...)                                                             \
  dariadb::utils::Exception::create_and_log(CODE_POS, __VA_ARGS__);                      \
  std::exit(1);
#else
#define THROW_EXCEPTION(...)                                                             \
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
    auto ss = std::string(_file) + " line: " + std::to_string(_line) + " function: " +
              std::string(_func) + "\n";
    return ss;
  }
  CodePos &operator=(const CodePos &) = delete;
};

class Exception : public std::exception {
public:
  template <typename... T>
  static Exception create_and_log(const CodePos &pos, T... message) {

    auto expanded_message = utils::strings::args_to_string(message...);
    auto ss = std::string("FATAL ERROR. The Exception will be thrown! ") +
              pos.toString() + " Message: " + expanded_message;
    logger_fatal(ss);
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
