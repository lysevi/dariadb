#include "time_ordered_set.h"
#include <algorithm>
#include <set>

using namespace memseries;
using namespace memseries::storage;

struct meas_time_compare{
	bool operator() (const memseries::Meas& lhs, const memseries::Meas& rhs) const {
		return lhs.time < rhs.time;
	}
};

class TimeOrderedSet::Private
{
public:
	typedef std::set<memseries::Meas,meas_time_compare> MeasSet;
	Private(const size_t max_size):
		_max_size(max_size),
		_count(0)
	{
	}
	
	Private(const Private &other) :
		_max_size(other._max_size),
		_count(other._count),
		_set(other._set)
	{
	}

	~Private() {
	}

	bool append(const memseries::Meas&m) {
		if (_count >= _max_size) {
			return false;
		}
		else {
			_set.insert(m);
			_count++;
			return true;
		}
	}

	memseries::Meas::MeasArray as_array()const {
		memseries::Meas::MeasArray result(_set.size());
		size_t pos = 0;
		for (auto&v : _set) {
			result[pos] = v;
			pos++;
		}
		return result;
	}

	size_t size()const {
		return _set.size();
	}
protected:
	size_t _max_size;
	size_t _count;
	MeasSet _set;
};

TimeOrderedSet::TimeOrderedSet() :_Impl(new TimeOrderedSet::Private(0))
{}

TimeOrderedSet::~TimeOrderedSet()
{}

TimeOrderedSet::TimeOrderedSet(const size_t max_size) :_Impl(new TimeOrderedSet::Private(max_size))
{}

TimeOrderedSet::TimeOrderedSet(const TimeOrderedSet & other): _Impl(new TimeOrderedSet::Private(*other._Impl))
{}

TimeOrderedSet::TimeOrderedSet(TimeOrderedSet && other): _Impl(std::move(other._Impl))
{}

void TimeOrderedSet::swap(TimeOrderedSet & other)throw()
{
	std::swap(_Impl, other._Impl);
}

TimeOrderedSet& TimeOrderedSet::operator=(const TimeOrderedSet & other)
{
	if (this != &other) {
		TimeOrderedSet tmp(other);
		this->swap(tmp);
	}
	return *this;
}

TimeOrderedSet& TimeOrderedSet::operator=(TimeOrderedSet && other)
{
	this->swap(other);
	return *this;
}

bool TimeOrderedSet::append(const Meas & m)
{
	return _Impl->append(m);
}

bool TimeOrderedSet::is_full() const
{
	return false;
}

memseries::Meas::MeasArray TimeOrderedSet::as_array()const {
	return _Impl->as_array();
}

size_t TimeOrderedSet::size()const {
	return _Impl->size();
}