#define BOOST_LOG_DYN_LINK 1
/*
When you identify the difference, you have to correct Boost building options and rebuild Boost. 
E.g. to set target Windows version to 7 define BOOST_USE_WINAPI_VERSION to 0x0601. 
If you don't want to change Windows version Boost is targeted for, 
you can define BOOST_USE_WINAPI_VERSION to 0x0501 while building your application, 
indicating that you want Boost to keep targeting XP even though your application is targeting 7.
*/
#define BOOST_USE_WINAPI_VERSION  0x0601
#include "logger.h"
#include <libdariadb/timeutil.h>
#include <boost/log/trivial.hpp>

ServerLogger::ServerLogger(const ServerLogger::Params &p) {
}

ServerLogger::~ServerLogger() {
}

void ServerLogger::message(dariadb::utils::LOG_MESSAGE_KIND kind, const std::string &msg) {
	switch (kind) {
	case dariadb::utils::LOG_MESSAGE_KIND::FATAL:
		BOOST_LOG_TRIVIAL(fatal) << msg;
		break;
	case dariadb::utils::LOG_MESSAGE_KIND::INFO:
		BOOST_LOG_TRIVIAL(info) << msg;
		break;
	case dariadb::utils::LOG_MESSAGE_KIND::MESSAGE:
		BOOST_LOG_TRIVIAL(debug) << msg;
		break;
	}
}