#include "time_ordered_set.h"
#include "utils.h"

#include <algorithm>
#include <set>
#include <limits>
#include <utility>

using namespace memseries;
using namespace memseries::storage;

TimeOrderedSet::TimeOrderedSet():TimeOrderedSet(0)
{}

TimeOrderedSet::~TimeOrderedSet()
{}

TimeOrderedSet::TimeOrderedSet(const size_t max_size):
	_max_size(max_size),
	_count(0),
	_minTime(std::numeric_limits<memseries::Time>::max()),
	_maxTime(std::numeric_limits<memseries::Time>::min())
{}

TimeOrderedSet::TimeOrderedSet(const TimeOrderedSet & other): 
	_max_size(other._max_size),
	_count(other._count),
	_set(other._set),
	_minTime(other._minTime),
	_maxTime(other._maxTime)	
{}


bool TimeOrderedSet::append(const Meas & m, bool force)
{
	if ((_count >= _max_size) && (!force)) {
		return false;
	}
	else {
		_set.insert(m);
		_minTime = std::min(_minTime, m.time);
		_maxTime = std::max(_maxTime, m.time);
		_count++;
		return true;
	}
}

bool TimeOrderedSet::is_full() const
{
	return _count >= _max_size;
}

memseries::Meas::MeasArray TimeOrderedSet::as_array()const {
	memseries::Meas::MeasArray result(_set.size());
	size_t pos = 0;
	for (auto&v : _set) {
		result[pos] = v;
		pos++;
	}
	return result;
}

size_t TimeOrderedSet::size()const {
	return _set.size();
}
memseries::Time TimeOrderedSet::minTime()const {
	return _minTime;
}

memseries::Time TimeOrderedSet::maxTime()const {
	return _maxTime;
}

bool TimeOrderedSet::inInterval(const memseries::Meas&m)const {
	return (utils::inInterval(_minTime, _maxTime, m.time))
		|| (_maxTime<m.time);
}
