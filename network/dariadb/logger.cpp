#include <spdlog/spdlog.h>

using spdlog::logger;

#include "logger.h"


ServerLogger::~ServerLogger() {}

ServerLogger::ServerLogger(const ServerLogger::Params &p):_params(p) {
	if (_params.use_stdout) {
		_logger = spdlog::stdout_logger_mt("dariadb", _params.color_console);
	}
	else {
		_logger = spdlog::daily_logger_mt("dariadb", "dariadb_logs", 0, 0);
	}
}

void ServerLogger::message(dariadb::utils::LOG_MESSAGE_KIND kind,
                           const std::string &msg) {
  switch (kind) {
  case dariadb::utils::LOG_MESSAGE_KIND::FATAL:
	  _logger->error(msg);
    break;
  case dariadb::utils::LOG_MESSAGE_KIND::INFO:
	  _logger->info(msg);
    break;
  case dariadb::utils::LOG_MESSAGE_KIND::MESSAGE:
	  _logger->debug(msg);
    break;
  }
  _logger->flush();
}
