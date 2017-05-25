#include <libdariadb/utils/logger.h>
#include <iostream>

using namespace dariadb::utils;
using namespace dariadb::utils::async;

std::shared_ptr<LogManager> LogManager::_instance = nullptr;
dariadb::utils::async::Locker LogManager::_locker;

void LogManager::start(ILogger_ptr &logger) {
  if (_instance == nullptr) {
    _instance = std::shared_ptr<LogManager>{new LogManager(logger)};
  }
}

void LogManager::stop() {
  _instance = nullptr;
}

LogManager *LogManager::instance() {
  auto tmp = _instance.get();
  if (tmp == nullptr) {
    std::lock_guard<Locker> lock(_locker);
    tmp = _instance.get();
    if (tmp == nullptr) {
      ILogger_ptr l = std::make_shared<ConsoleLogger>();
      _instance = std::make_shared<LogManager>(l);
	  tmp = _instance.get();
    }
  }
  return tmp;
}

LogManager::LogManager(ILogger_ptr &logger) {
  _logger = logger;
}

void LogManager::message(LOG_MESSAGE_KIND kind, const std::string &msg) {
  std::lock_guard<utils::async::Locker> lg(_msg_locker);
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
