#pragma once

#include "meas.h"
#include <memory>

namespace memseries {
	namespace storage {

		class TimeOrderedSet {
		public:
			TimeOrderedSet();
			~TimeOrderedSet();
			TimeOrderedSet(const size_t max_size);
			TimeOrderedSet(const TimeOrderedSet&other);
			TimeOrderedSet(TimeOrderedSet&&other);
			void swap(TimeOrderedSet&other) throw();
			TimeOrderedSet& operator=(const TimeOrderedSet&other);
			TimeOrderedSet& operator=(TimeOrderedSet&&other);

			bool append(const Meas&m);
			bool is_full() const;
			//TODO must return ref or write to param (pointer to array)
			memseries::Meas::MeasArray as_array()const;
			size_t size()const;
		protected:
			class Private;
			std::unique_ptr<Private> _Impl;
		};

	};
};