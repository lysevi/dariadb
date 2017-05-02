#include "server_logger.h"

ServerLogger::Params::Params() {
  use_stdout = true;
  //#if defined(_DEBUG) || defined(DEBUG) || defined(NDEBUG)
  //  dbg_logging = true;
  //#else
  dbg_logging = false;
//#endif
#ifdef _WIN32
  color_console = false;
#else
  color_console = true;
#endif
}

ServerLogger::~ServerLogger() {}

ServerLogger::ServerLogger(const ServerLogger::Params &p) : _params(p) {
  if (_params.use_stdout) {
    if (_params.color_console) {
      _logger = spdlog::stdout_color_mt("dariadb");
    } else {
      _logger = spdlog::stdout_logger_mt("dariadb");
    }
  } else {
    _logger = spdlog::daily_logger_mt("dariadb", "dariadb_log", 0, 0);
  }
  _logger->set_level(spdlog::level::debug);
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
    if (_params.dbg_logging) {
      _logger->debug(msg);
    }
    break;
  }
  _logger->flush();
}
