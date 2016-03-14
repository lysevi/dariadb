#pragma once

#include "meas.h"
#include <memory>

namespace memseries {
	namespace storage {

		struct meas_time_compare {
			bool operator() (const memseries::Meas& lhs, const memseries::Meas& rhs) const {
				return lhs.time < rhs.time;
			}
		};

		class TimeOrderedSet {
			typedef std::set<memseries::Meas, meas_time_compare> MeasSet;
		public:
			TimeOrderedSet();
			~TimeOrderedSet();
			TimeOrderedSet(const size_t max_size);
			TimeOrderedSet(const TimeOrderedSet&other);
			TimeOrderedSet& operator=(const TimeOrderedSet&other)=default;

            bool append(const Meas&m, bool force=false);
			bool is_full() const;
			//TODO must return ref or write to param (pointer to array)
			memseries::Meas::MeasArray as_array()const;
			size_t size()const;
			memseries::Time minTime()const;
			memseries::Time maxTime()const;
			bool inInterval(const memseries::Meas&m)const;
		protected:
			size_t _max_size;
			size_t _count;
			MeasSet _set;

			memseries::Time _minTime;
			memseries::Time _maxTime;
		};
	}
}
