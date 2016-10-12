#pragma once

#include "libdariadb/utils/locker.h"
#include "libdariadb/utils/strings.h"
#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>

namespace dariadb {
namespace utils {

enum class LOG_MESSAGE_KIND { MESSAGE, INFO, FATAL };

class ILogger {
public:
  virtual void message(LOG_MESSAGE_KIND kind, const std::string &msg) = 0;
    virtual ~ILogger(){}
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

  template <typename... T> void variadic_message(LOG_MESSAGE_KIND kind, T... args) {
    auto str_message = utils::strings::args_to_string(args...);
    this->message(kind, str_message);
  }
private:
  static std::shared_ptr<LogManager> _instance;
  static utils::Locker _locker;
  utils::Locker _msg_locker;
  ILogger_ptr _logger;
};
}

template <typename... T> void logger(T &&... args) {
  dariadb::utils::LogManager::instance()->variadic_message(
      dariadb::utils::LOG_MESSAGE_KIND::MESSAGE, args...);
}

template <typename... T> void logger_info(T &&... args) {
  dariadb::utils::LogManager::instance()->variadic_message(
      dariadb::utils::LOG_MESSAGE_KIND::INFO, args...);
}

template <typename... T> void logger_fatal(T &&... args) {
  dariadb::utils::LogManager::instance()->variadic_message(
      dariadb::utils::LOG_MESSAGE_KIND::FATAL, args...);
}
}
