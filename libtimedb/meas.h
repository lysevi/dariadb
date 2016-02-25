#pragma once

#include <memory>
#include <vector>
#include <list>
#include <ctime>
#include <cstring>
#include <set>
#include <array>
#include "common.h"

namespace timedb {
	typedef uint64_t Time;
	typedef uint64_t Id;
	typedef uint64_t Flag;
	typedef uint64_t Value;
	typedef std::vector<Id> IdArray;
	typedef std::set<Id> IdSet;

	struct Meas {
		typedef Meas *PMeas;
		typedef std::vector<Meas> MeasArray;
		typedef std::list<Meas> MeasList;

		static Meas empty();

		Meas();
		void readFrom(const Meas::PMeas m);
		bool operator==(const Meas&other)const {
			return id == other.id&&time == other.time&&flag == other.flag&&value == other.value;
		}

		Id id;
		Time time;
		Flag flag;
		Value value;
	};

	bool in_filter(Flag filter, Flag flg);
	
}
