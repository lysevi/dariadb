#pragma once

#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <sstream>

#define logger(...)                                                            \
  dariadb::utils::LogManager::instance()->variadic_message(                             \
      dariadb::utils::LOG_MESSAGE_KIND::MESSAGE, __VA_ARGS__);
#define logger_info(...)                                                       \
  dariadb::utils::LogManager::instance()->variadic_message(                             \
      dariadb::utils::LOG_MESSAGE_KIND::INFO, __VA_ARGS__);
#define logger_fatal(...)                                                      \
  dariadb::utils::LogManager::instance()->variadic_message(                             \
      dariadb::utils::LOG_MESSAGE_KIND::FATAL, __VA_ARGS__);

namespace dariadb {
namespace utils {

enum class LOG_MESSAGE_KIND { MESSAGE, INFO, FATAL };

class ILogger {
public:
  virtual void message(LOG_MESSAGE_KIND kind, const std::string &msg) = 0;
};

using ILogger_ptr = std::shared_ptr<ILogger>;

class ConsoleLogger : public ILogger {
public:
  void message(LOG_MESSAGE_KIND kind, const std::string &msg) override;
};

class LogManager {
  LogManager(ILogger_ptr &logger);

public:
  static bool start(); /// return false if logger was already started.
  static void start(ILogger_ptr &logger);
  static void stop();
  static LogManager *instance() { return _instance; }

  void message(LOG_MESSAGE_KIND kind, const std::string &msg);

  template<typename...T>
  void variadic_message(LOG_MESSAGE_KIND kind, T...args){
    auto str_message=args_to_string(args...);
    this->message(kind,str_message);
  }

  template<class Head>
  void args_as_string(std::ostream& s, Head&& head) {
      s <<std::forward<Head>(head);
  }

  template<class Head, class... Tail>
  void args_as_string(std::ostream& s, Head&& head, Tail&&... tail) {
      s << std::forward<Head>(head);
      args_as_string(s, std::forward<Tail>(tail)...);
  }

  template<class... Args>
  std::string args_to_string(Args&&... args) {
      std::stringstream ss;
      args_as_string(ss, std::forward<Args>(args)...);
      return ss.str();
  }

private:
  static LogManager *_instance;
  ILogger_ptr _logger;
};
}
}
