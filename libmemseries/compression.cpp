#include "compression.h"
#include "utils.h"
#include "exception.h"

#include <sstream>
#include <cassert>
#include <limits>

using namespace memseries::compression;

CopmressedWriter::CopmressedWriter(BinaryBuffer bw_time, BinaryBuffer bw_values, BinaryBuffer bw_flags):
	time_comp(bw_time),
	value_comp(bw_values),
	flag_comp(bw_flags)
{
	_is_first = true;
	_is_full = false;
}

CopmressedWriter::~CopmressedWriter()
{}

CopmressedWriter::CopmressedWriter(const CopmressedWriter &other):

    time_comp(other.time_comp),
    value_comp(other.value_comp),
    flag_comp(other.flag_comp)

{
    _is_first=other._is_first;
    _is_full=other._is_full;
}

void CopmressedWriter::swap(CopmressedWriter &other){
    std::swap(time_comp,other.time_comp);
    std::swap(value_comp,other.value_comp);
    std::swap(flag_comp,other.flag_comp);
    std::swap(_is_first,other._is_first);
    std::swap(_is_full,other._is_full);
}

CopmressedWriter& CopmressedWriter::operator=(CopmressedWriter &other){
    CopmressedWriter temp(other);
    this->swap(other);
    return *this;
}

CopmressedWriter& CopmressedWriter::operator=(CopmressedWriter &&other){
    CopmressedWriter temp(std::move(other));
    this->swap(temp);
    return *this;
}

bool CopmressedWriter::append(const Meas&m) {
	if (_is_first) {
		_first = m;
		_is_first = false;
	}
	
	if (_first.id != m.id) {
		std::stringstream ss{};
		ss << "(_first.id != m.id)" << " id:" << m.id << " first.id:" << _first.id;
		throw std::logic_error(ss.str().c_str());
	}
	if (time_comp.is_full() || value_comp.is_full() || flag_comp.is_full()) {
		_is_full = true;
		return false;
	}
	auto t_f = time_comp.append(m.time);
	auto f_f = value_comp.append(m.value);
	auto v_f = flag_comp.append(m.flag);

	if (!t_f || !f_f || !v_f) {
		_is_full = true;
		return false;
	}
	else {
		return true;
	}
}

CopmressedReader::CopmressedReader(BinaryBuffer bw_time, BinaryBuffer bw_values, BinaryBuffer bw_flags, Meas first):
	time_dcomp(bw_time,first.time),
	value_dcomp(bw_values, first.value),
	flag_dcomp(bw_flags, first.flag)
{
	_first = first;
}

CopmressedReader::~CopmressedReader()
{
}

memseries::Meas CopmressedReader::read()
{
	Meas result{};
	result.time = time_dcomp.read();
	result.value = value_dcomp.read();
	result.flag = flag_dcomp.read();
	return result;
}
