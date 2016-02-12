#pragma once
#include <string>
#include <cstdint>
#include <iostream>

#define logger(msg)       timedb::utils::Logger::get()->GetStream()<<"   "<<msg<<std::endl
#define logger_info(msg)  timedb::utils::Logger::get()->GetStream()<<"   "<<msg<<std::endl
#define logger_fatal(msg) timedb::utils::Logger::get()->GetStream()<<"   "<<msg<<std::endl

namespace timedb
{
	namespace utils
	{
		class Logger
		{
		public:
			static Logger* get();
			virtual std::ostream& GetStream();

			virtual ~Logger() {
			};
		private:
			uint16_t m_level;
			Logger() {
			};
		};
	}
}
