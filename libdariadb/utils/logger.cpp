#include "logger.h"
#include <iostream>

using namespace dariadb::utils;

std::shared_ptr<LogManager> LogManager::_instance = nullptr;
Locker LogManager::_locker;

void LogManager::start(ILogger_ptr &logger) {
  if (_instance == nullptr) {
    _instance = std::shared_ptr<LogManager>{new LogManager(logger)};
  }
}

LogManager *LogManager::instance() {
  auto tmp = _instance.get();
  if (tmp == nullptr) {
    std::lock_guard<Locker> lock(_locker);
    tmp = _instance.get();
    if (tmp == nullptr) {
      ILogger_ptr l{new ConsoleLogger};
      tmp = new LogManager(l);
      _instance = std::shared_ptr<LogManager>{tmp};
    }
  }
  return tmp;
}

LogManager::LogManager(ILogger_ptr &logger) {
  _logger = logger;
}

void LogManager::message(LOG_MESSAGE_KIND kind, const std::string &msg) {
  std::lock_guard<utils::Locker> lg(_msg_locker);
  _logger->message(kind, msg);
}

void ConsoleLogger::message(LOG_MESSAGE_KIND kind, const std::string &msg) {
  switch (kind) {
  case LOG_MESSAGE_KIND::FATAL:
    std::cerr << "[err] " << msg << std::endl;
    break;
  case LOG_MESSAGE_KIND::INFO:
    std::cout << "[inf] " << msg << std::endl;
    break;
  case LOG_MESSAGE_KIND::MESSAGE:
    std::cout << "[dbg] " << msg << std::endl;
    break;
  }
}
