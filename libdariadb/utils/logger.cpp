#include "logger.h"

using namespace dariadb::utils;

LogManager *LogManager::_instance = nullptr;

bool LogManager::start() {
  if (_instance == nullptr) {
    ILogger_ptr l{new ConsoleLogger};
    _instance = new LogManager(l);
    return true;
  }
  return false;
}

void LogManager::start(ILogger_ptr &logger) {
  if (_instance == nullptr) {
    _instance = new LogManager(logger);
  }
}

void LogManager::stop() {
  delete _instance;
  _instance = nullptr;
}

LogManager::LogManager(ILogger_ptr &logger) { _logger = logger; }

void LogManager::message(LOG_MESSAGE_KIND kind, const std::string &msg) {
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
    std::cout << "      " << msg << std::endl;
    break;
  }
}
