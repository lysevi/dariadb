#include "logger.h"

ServerLogger::ServerLogger(const ServerLogger::Params &p) {}

ServerLogger::~ServerLogger() {}

#ifdef ENABLE_BOOST_LOGGER
#define BOOST_LOG_DYN_LINK 1
/*
When you identify the difference, you have to correct Boost building options and
rebuild Boost.
E.g. to set target Windows version to 7 define BOOST_USE_WINAPI_VERSION to
0x0601.
If you don't want to change Windows version Boost is targeted for,
you can define BOOST_USE_WINAPI_VERSION to 0x0501 while building your
application,
indicating that you want Boost to keep targeting XP even though your application
is targeting 7.
*/
#define BOOST_USE_WINAPI_VERSION 0x0601

#include <boost/log/trivial.hpp>

void ServerLogger::message(dariadb::utils::LOG_MESSAGE_KIND kind,
                           const std::string &msg) {
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
#else
#include <iostream>

#include <libdariadb/timeutil.h>

void ServerLogger::message(dariadb::utils::LOG_MESSAGE_KIND kind,
                           const std::string &msg) {
  auto ct = dariadb::timeutil::current_time();
  auto ct_str = dariadb::timeutil::to_string(ct);
  std::stringstream ss;
  ss << ct_str << " ";
  switch (kind) {
  case dariadb::utils::LOG_MESSAGE_KIND::FATAL:
    ss << "[err] " << msg << std::endl;
    break;
  case dariadb::utils::LOG_MESSAGE_KIND::INFO:
    ss << "[inf] " << msg << std::endl;
    break;
  case dariadb::utils::LOG_MESSAGE_KIND::MESSAGE:
    ss << "[dbg] " << msg << std::endl;
    break;
  }
  std::cout << ss.str();
}

#endif
