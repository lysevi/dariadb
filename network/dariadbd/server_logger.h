#pragma once
#include <libdariadb/utils/logger.h>
#include <memory>
#include <spdlog/spdlog.h>

class ServerLogger : public dariadb::utils::ILogger {
public:
  struct Params {
    bool use_stdout;
    bool color_console;
    bool dbg_logging;
    Params();
  };
  ServerLogger(const ServerLogger::Params &p);
  ~ServerLogger();
  void message(dariadb::utils::LOG_MESSAGE_KIND kind, const std::string &msg);

private:
  std::shared_ptr<spdlog::logger> _logger;
  Params _params;
};
