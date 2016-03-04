#pragma once

#include "meas.h"
#include <memory>


namespace memseries{
	namespace storage {

		class Reader
		{
		public:
			virtual bool isEnd() const = 0;
			virtual void readNext(Meas::MeasList*output) = 0;
			virtual void readAll(Meas::MeasList*output);
		};

		typedef std::shared_ptr<Reader> Reader_ptr;
		class AbstractStorage {
		public:
			virtual ~AbstractStorage() = default;
			/// min time of writed meas
			virtual Time minTime() = 0;
			/// max time of writed meas
			virtual Time maxTime() = 0;


			virtual append_result append(const Meas::MeasArray& ma);
			virtual append_result append(const Meas::MeasList& ml);
			virtual append_result append(const Meas::PMeas begin, const size_t size) = 0;
            virtual append_result append(const Meas& value) = 0;

			virtual Reader_ptr readInterval(Time from, Time to);
			virtual Reader_ptr readInTimePoint(Time time_point);

            virtual Reader_ptr readInterval(const IdArray &ids, Flag flag, Time from, Time to) = 0;
			virtual Reader_ptr readInTimePoint(const IdArray &ids, Flag flag, Time time_point) = 0;
		};
	}
}
