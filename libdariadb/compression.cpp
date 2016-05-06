#include "compression.h"
#include "compression/delta.h"
#include "compression/flag.h"
#include "compression/xor.h"
#include "utils/exception.h"
#include "utils/utils.h"

#include <algorithm>
#include <cassert>
#include <limits>
#include <sstream>
using namespace dariadb;
using namespace dariadb::compression;

class CopmressedWriter::Private {
public:
  Private(const BinaryBuffer_Ptr &bw)
      : time_comp(bw), value_comp(bw), flag_comp(bw), src_comp(bw) {
    _is_first = true;
    _is_full = false;
  }

  bool append(const Meas &m) {
    if (_is_first) {
      _first = m;
      _is_first = false;
    }

    if (_first.id != m.id) {
      std::stringstream ss{};
      ss << "(_first.id != m.id)"
         << " id:" << m.id << " first.id:" << _first.id;
      throw MAKE_EXCEPTION(ss.str().c_str());
    }
    if (_is_full || time_comp.is_full() || value_comp.is_full() ||
        flag_comp.is_full() || src_comp.is_full()) {
      _is_full = true;
      return false;
    }

    auto t_f = time_comp.append(m.time);
    auto f_f = value_comp.append(m.value);
    auto v_f = flag_comp.append(m.flag);
    auto s_f = src_comp.append(m.src);

    if (!t_f || !f_f || !v_f || !s_f) {
      _is_full = true;
      return false;
    } else {
      return true;
    }
  }

  bool is_full() const { return _is_full; }

  size_t used_space() const { return time_comp.used_space(); }

  CopmressedWriter::Position get_position() const {
    CopmressedWriter::Position result;
    result.first = _first;
    result.time_pos = time_comp.get_position();
    result.value_pos = value_comp.get_position();
    result.flag_pos = flag_comp.get_position();
    result.src_pos = src_comp.get_position();
    result.is_first = _is_first;
    result.is_full = _is_full;
    return result;
  }

  void restore_postion(const CopmressedWriter::Position &pos) {
    _first = pos.first;
    _is_first = pos.is_first;
    _is_full = pos.is_full;

    time_comp.restore_position(pos.time_pos);
    value_comp.restore_position(pos.value_pos);
    flag_comp.restore_position(pos.flag_pos);
    src_comp.restore_position(pos.src_pos);
  }

protected:
  Meas _first;
  bool _is_first;
  bool _is_full;
  DeltaCompressor time_comp;
  XorCompressor value_comp;
  FlagCompressor flag_comp;
  FlagCompressor src_comp;
};

class CopmressedReader::Private {
public:
  Private(const BinaryBuffer_Ptr &bw, const Meas &first)
      : time_dcomp(bw, first.time), value_dcomp(bw, first.value),
        flag_dcomp(bw, first.flag), src_dcomp(bw, first.src) {
    _first = first;
  }

  Meas read() {
    Meas result{};
    result.time = time_dcomp.read();
    result.value = value_dcomp.read();
    result.flag = flag_dcomp.read();
    result.src = src_dcomp.read();
    result.id = _first.id;
    return result;
  }

  bool is_full() const {
    return this->time_dcomp.is_full() || this->value_dcomp.is_full() ||
           this->flag_dcomp.is_full() || this->src_dcomp.is_full();
  }

protected:
  dariadb::Meas _first;

  DeltaDeCompressor time_dcomp;
  XorDeCompressor value_dcomp;
  FlagDeCompressor flag_dcomp;
  FlagDeCompressor src_dcomp;
};

CopmressedWriter::CopmressedWriter() {
  this->_Impl = nullptr;
}

CopmressedWriter::CopmressedWriter(const BinaryBuffer_Ptr &bw)
    : _Impl(new CopmressedWriter::Private(bw)) {}

CopmressedWriter::~CopmressedWriter() {}

CopmressedWriter::CopmressedWriter(const CopmressedWriter &other)
    : _Impl(new CopmressedWriter::Private(*other._Impl)) {}

void CopmressedWriter::swap(CopmressedWriter &other) {
  std::swap(_Impl, other._Impl);
}

CopmressedWriter &CopmressedWriter::operator=(const CopmressedWriter &other) {
  if (this == &other) {
    return *this;
  }
  CopmressedWriter temp(other);
  std::swap(this->_Impl, temp._Impl);
  return *this;
}

CopmressedWriter &CopmressedWriter::operator=(CopmressedWriter &&other) {
  if (this == &other) {
    return *this;
  }
  std::swap(_Impl, other._Impl);
  return *this;
}

bool CopmressedWriter::append(const Meas &m) {
  return _Impl->append(m);
}

bool CopmressedWriter::is_full() const {
  return _Impl->is_full();
}

size_t CopmressedWriter::used_space() const {
  return _Impl->used_space();
}

CopmressedWriter::Position CopmressedWriter::get_position() const {
  return _Impl->get_position();
}

void CopmressedWriter::restore_position(const CopmressedWriter::Position &pos) {
  _Impl->restore_postion(pos);
}

CopmressedReader::CopmressedReader() {
  this->_Impl = nullptr;
}

CopmressedReader::CopmressedReader(const BinaryBuffer_Ptr &bw,
                                   const Meas &first)
    : _Impl(new CopmressedReader::Private(bw, first)) {}

CopmressedReader::~CopmressedReader() {}

dariadb::Meas CopmressedReader::read() {
  return _Impl->read();
}

bool CopmressedReader::is_full() const {
  return _Impl->is_full();
}

CopmressedReader::CopmressedReader(const CopmressedReader &other)
    : _Impl(new CopmressedReader::Private(*other._Impl)) {}

void CopmressedReader::swap(CopmressedReader &other) {
  std::swap(_Impl, other._Impl);
}

CopmressedReader &CopmressedReader::operator=(const CopmressedReader &other) {
  if (this == &other) {
    return *this;
  }
  CopmressedReader temp(other);
  std::swap(this->_Impl, temp._Impl);
  return *this;
}

CopmressedReader &CopmressedReader::operator=(CopmressedReader &&other) {
  if (this == &other) {
    return *this;
  }
  std::swap(_Impl, other._Impl);
  return *this;
}
