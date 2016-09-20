#include "logger.h"

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

#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/sinks/text_file_backend.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/sources/record_ostream.hpp>
#include <boost/log/attributes/scoped_attribute.hpp>
#include <boost/log/support/date_time.hpp>

#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/thread.hpp>

namespace logging = boost::log;
namespace src = boost::log::sources;
namespace sinks = boost::log::sinks;
namespace keywords = boost::log::keywords;
namespace expr = boost::log::expressions;

ServerLogger::ServerLogger(const ServerLogger::Params &p) {
	if (!p.use_stdout) {
		logging::add_file_log
		(
			keywords::file_name = "dariadb_%Y%m%d.%3N.log",                     /*< file name pattern >*/
			keywords::rotation_size = 10 * 1024 * 1024,                                   /*< rotate files every 10 MiB... >*/
			keywords::time_based_rotation = sinks::file::rotation_at_time_point(0, 0, 0), /*< ...or at midnight >*/
			keywords::format =
			(
				expr::stream
				//<< expr::attr< unsigned int >("LineID")
				<< "["<<expr::format_date_time<boost::posix_time::ptime>("TimeStamp", "%Y-%m-%d %H:%M:%S")
				<<"] [" << expr::attr< boost::thread::id >("ThreadID")
				<<"] ["<< logging::trivial::severity<< "] " << expr::smessage
				)
		);
		
		logging::core::get()->set_filter
		(
			logging::trivial::severity >= logging::trivial::info
		);
		logging::add_common_attributes();
	}
}

void ServerLogger::message(dariadb::utils::LOG_MESSAGE_KIND kind,
                           const std::string &msg) {
  BOOST_LOG_SCOPED_THREAD_TAG("ThreadID", boost::this_thread::get_id());
  using namespace logging::trivial;
  src::severity_logger< severity_level > lg;

  switch (kind) {
  case dariadb::utils::LOG_MESSAGE_KIND::FATAL:
	  BOOST_LOG_SEV(lg, fatal) << msg;
    break;
  case dariadb::utils::LOG_MESSAGE_KIND::INFO:
	  BOOST_LOG_SEV(lg, info) << msg;
    break;
  case dariadb::utils::LOG_MESSAGE_KIND::MESSAGE:
	  BOOST_LOG_SEV(lg, debug) << msg;
    break;
  }
}
#else
#include <iostream>

#include <libdariadb/timeutil.h>

ServerLogger::ServerLogger(const ServerLogger::Params &p) {}

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
