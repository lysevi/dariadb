#pragma once
#include <libdariadb/utils/logger.h>

class ServerLogger : public dariadb::utils::ILogger {
public:
	struct Params{
		bool use_stdout;
	};
	//TODO path to file
	ServerLogger(const ServerLogger::Params &p);
	~ServerLogger();
	void message(dariadb::utils::LOG_MESSAGE_KIND kind, const std::string &msg);
};