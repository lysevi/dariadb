#include "compression.h"
#include "compression/delta.h"
#include "compression/xor.h"
#include "compression/flag.h"
#include "utils.h"
#include "exception.h"

#include <sstream>
#include <cassert>
#include <limits>

using namespace memseries::compression;

class CopmressedWriter::Impl {
public:
    Impl() = default;
    Impl(BinaryBuffer bw_time, BinaryBuffer bw_values, BinaryBuffer bw_flags):
        time_comp(bw_time),
        value_comp(bw_values),
        flag_comp(bw_flags)
    {
        _is_first = true;
        _is_full = false;
    }

    ~Impl(){}
    Impl(const Impl &other):
        time_comp(other.time_comp),
        value_comp(other.value_comp),
        flag_comp(other.flag_comp)
    {
        _is_first=other._is_first;
        _is_full=other._is_full;
    }
    void swap(Impl &other){
        std::swap(time_comp,other.time_comp);
        std::swap(value_comp,other.value_comp);
        std::swap(flag_comp,other.flag_comp);
        std::swap(_is_first,other._is_first);
        std::swap(_is_full,other._is_full);
    }

    bool append(const Meas &m){
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

    bool is_full() const { return _is_full; }

protected:
    Meas _first;
    bool _is_first;
    bool _is_full;
    DeltaCompressor time_comp;
    XorCompressor value_comp;
    FlagCompressor flag_comp;
};

class CopmressedReader::Impl {
public:
    Impl() = default;
    Impl(BinaryBuffer bw_time,
         BinaryBuffer bw_values,
         BinaryBuffer bw_flags, Meas first):

        time_dcomp(bw_time,first.time),
        value_dcomp(bw_values, first.value),
        flag_dcomp(bw_flags, first.flag)
    {
        _first = first;
    }

    ~Impl(){}

    Meas read(){
        Meas result{};
        result.time = time_dcomp.read();
        result.value = value_dcomp.read();
        result.flag = flag_dcomp.read();
        return result;
    }

    bool is_full() const {
        return this->time_dcomp.is_full() || this->value_dcomp.is_full() ||
                this->flag_dcomp.is_full();
    }

protected:
    memseries::Meas _first;

    DeltaDeCompressor time_dcomp;
    XorDeCompressor value_dcomp;
    FlagDeCompressor flag_dcomp;
};


CopmressedWriter::CopmressedWriter(BinaryBuffer bw_time, BinaryBuffer bw_values, BinaryBuffer bw_flags)
{
    _Impl=new CopmressedWriter::Impl(bw_time,bw_values,bw_flags);
}

CopmressedWriter::~CopmressedWriter()
{
    delete _Impl;
}

CopmressedWriter::CopmressedWriter(const CopmressedWriter &other){
    _Impl=new CopmressedWriter::Impl(*other._Impl);
}

void CopmressedWriter::swap(CopmressedWriter &other){
    std::swap(_Impl,other._Impl);
}

CopmressedWriter& CopmressedWriter::operator=(CopmressedWriter &other){
    if(this==&other){
        return *this;
    }
    CopmressedWriter temp(other);
    std::swap(this->_Impl,temp._Impl);
    return *this;
}

CopmressedWriter& CopmressedWriter::operator=(CopmressedWriter &&other){
    if(this==&other){
        return *this;
    }
    std::swap(_Impl,other._Impl);
    return *this;
}

bool CopmressedWriter::append(const Meas&m){
    return _Impl->append(m);
}

bool CopmressedWriter::is_full()const{
    return _Impl->is_full();
}

CopmressedReader::CopmressedReader(BinaryBuffer bw_time, BinaryBuffer bw_values, BinaryBuffer bw_flags, Meas first)
{
    _Impl=new CopmressedReader::Impl(bw_time,bw_values,bw_flags,first);
}

CopmressedReader::~CopmressedReader()
{
    delete _Impl;
}

memseries::Meas CopmressedReader::read()
{
    return _Impl->read();
}

bool CopmressedReader::is_full()const{
    return _Impl->is_full();
}

CopmressedReader::CopmressedReader(const CopmressedReader &other){
    _Impl=new CopmressedReader::Impl(*other._Impl);
}

void CopmressedReader::swap(CopmressedReader &other){
    std::swap(_Impl,other._Impl);
}

CopmressedReader& CopmressedReader::operator=(CopmressedReader &other){
    if(this==&other){
        return *this;
    }
    CopmressedReader temp(other);
    std::swap(this->_Impl,temp._Impl);
    return *this;
}

CopmressedReader& CopmressedReader::operator=(CopmressedReader &&other){
    if(this==&other){
        return *this;
    }
    std::swap(_Impl,other._Impl);
    return *this;
}
