#include "time_ordered_set.h"
#include "../utils/utils.h"

#include <algorithm>
#include <set>
#include <limits>
#include <utility>
#include <cassert>

using namespace dariadb;
using namespace dariadb::storage;

TimeOrderedSet::TimeOrderedSet():TimeOrderedSet(0)
{
	is_dropped = false;
}

TimeOrderedSet::~TimeOrderedSet()
{}

TimeOrderedSet::TimeOrderedSet(const size_t max_size):
	_max_size(max_size),
	_count(0),
	_minTime(std::numeric_limits<dariadb::Time>::max()),
	_maxTime(std::numeric_limits<dariadb::Time>::min())
{
	is_dropped = false;
}

TimeOrderedSet::TimeOrderedSet(const TimeOrderedSet & other): 
    is_dropped(other.is_dropped),
	_max_size(other._max_size),
	_count(other._count),
	_set(other._set),
	_minTime(other._minTime),
    _maxTime(other._maxTime)
{}

TimeOrderedSet& TimeOrderedSet::operator=(const TimeOrderedSet&other) {
	if (this != &other) {
		_max_size = other._max_size;
		_count = other._count;
		_set = other._set;
		_minTime = other._minTime;
		_maxTime = other._maxTime;
		is_dropped = other.is_dropped;
	}
	return *this;
}

bool TimeOrderedSet::append(const Meas & m, bool force){
    std::lock_guard<std::mutex> lg(_locker);
	assert(!is_dropped);
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

bool TimeOrderedSet::is_full() const{
	return _count >= _max_size || is_dropped;
}

dariadb::Meas::MeasArray TimeOrderedSet::as_array()const {
	// locks dont need, becase as_array calls when when write window great than this->maxTime
	//_locker.lock();
	dariadb::Meas::MeasArray result(_set.begin(),_set.end());
	//_locker.unlock();
	return result;
}

size_t TimeOrderedSet::size()const {
	return _set.size();
}
dariadb::Time TimeOrderedSet::minTime()const {
	return _minTime;
}

dariadb::Time TimeOrderedSet::maxTime()const {
	return _maxTime;
}

bool TimeOrderedSet::inInterval(const dariadb::Meas&m)const {
	return (utils::inInterval(_minTime, _maxTime, m.time))
		|| (_maxTime<m.time);
}
