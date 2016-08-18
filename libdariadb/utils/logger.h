#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <sstream>
#include <atomic>
#include <mutex>
#include "locker.h"

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
  static void start(ILogger_ptr &logger);
  static LogManager *instance();

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
  static std::shared_ptr<LogManager>_instance;
  static utils::Locker _locker;
  ILogger_ptr _logger;
};
}

template<typename...T>
void logger(T...args){
    dariadb::utils::LogManager::instance()->variadic_message(
        dariadb::utils::LOG_MESSAGE_KIND::MESSAGE,args...);
}

template<typename...T>
void logger_info(T...args){
    dariadb::utils::LogManager::instance()->variadic_message(
        dariadb::utils::LOG_MESSAGE_KIND::INFO,args...);
}

template<typename...T>
void logger_fatal(T...args){
    dariadb::utils::LogManager::instance()->variadic_message(
        dariadb::utils::LOG_MESSAGE_KIND::FATAL,args...);
}

}
