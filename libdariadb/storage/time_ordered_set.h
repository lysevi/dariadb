#pragma once

#include "../meas.h"
#include "../utils/locker.h"
#include <memory>

namespace dariadb {
	namespace storage {

		struct meas_time_compare {
			bool operator() (const dariadb::Meas& lhs, const dariadb::Meas& rhs) const {
				return lhs.time < rhs.time;
			}
		};

		class TimeOrderedSet {
			typedef std::set<dariadb::Meas, meas_time_compare> MeasSet;
		public:
			TimeOrderedSet();
			~TimeOrderedSet();
			TimeOrderedSet(const size_t max_size);
			TimeOrderedSet(const TimeOrderedSet&other);
			TimeOrderedSet& operator=(const TimeOrderedSet&other);

            bool append(const Meas&m, bool force=false);
			bool is_full() const;
			//TODO must return ref or write to param (pointer to array)
			dariadb::Meas::MeasArray as_array()const;
			size_t size()const;
			dariadb::Time minTime()const;
			dariadb::Time maxTime()const;
			bool inInterval(const dariadb::Meas&m)const;

			bool is_dropped;
		protected:
			size_t _max_size;
			size_t _count;
			MeasSet _set;

			dariadb::Time _minTime;
			dariadb::Time _maxTime;
            mutable dariadb::utils::Locker _locker;
		};
	}
}
