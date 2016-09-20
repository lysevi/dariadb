#pragma once
#include <memory>
#include <libdariadb/utils/logger.h>

class logger;

class ServerLogger : public dariadb::utils::ILogger {
public:
	struct Params{
		bool use_stdout;
		bool color_console;
		Params() {
			use_stdout = true;
#ifdef _WIN32
			color_console = false;
#else
			color_console = true;
#endif
		}
	};
	ServerLogger(const ServerLogger::Params &p);
	~ServerLogger();
	void message(dariadb::utils::LOG_MESSAGE_KIND kind, const std::string &msg);
private:
	std::shared_ptr<logger> _logger;
	Params _params;
};