#pragma once

#include <libdariadb/st_exports.h>
#include <libdariadb/utils/async/locker.h>
#include <libdariadb/utils/strings.h>
#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>

namespace dariadb {
namespace utils {

enum class LOG_MESSAGE_KIND { MESSAGE, INFO, FATAL };

class ILogger {
public:
  virtual void message(LOG_MESSAGE_KIND kind, const std::string &msg) = 0;
  virtual ~ILogger() {}
};

using ILogger_ptr = std::shared_ptr<ILogger>;

class ConsoleLogger : public ILogger {
public:
  EXPORT void message(LOG_MESSAGE_KIND kind, const std::string &msg) override;
};

class LogManager {
public:
  LogManager(ILogger_ptr &logger);

  EXPORT static void start(ILogger_ptr &logger);
  EXPORT static void stop();
  EXPORT static LogManager *instance();

  EXPORT void message(LOG_MESSAGE_KIND kind, const std::string &msg);

  template <typename... T> void variadic_message(LOG_MESSAGE_KIND kind, T&&... args) {
    auto str_message = utils::strings::args_to_string(args...);
    this->message(kind, str_message);
  }

private:
  static std::shared_ptr<LogManager> _instance;
  static utils::async::Locker _locker;
  utils::async::Locker _msg_locker;
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
