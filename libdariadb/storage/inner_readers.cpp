#include "inner_readers.h"
#include "../flags.h"
#include <cassert>

using namespace dariadb;
using namespace dariadb::compression;
using namespace dariadb::storage;

InnerReader::InnerReader(dariadb::Flag flag, dariadb::Time from, dariadb::Time to)
    : _values{}, _flag(flag), _from(from), _to(to) {
  end = false;
}

bool InnerReader::isEnd() const {
  return this->end;
}

dariadb::IdArray InnerReader::getIds() const {
  return _ids;
}

void InnerReader::readNext(storage::ReaderClb *clb) {
  std::lock_guard<std::mutex> lg(_locker);

  for (auto id : this->_ids) {
    for (auto sub : _values) {
      if (sub.id == id) {
        if (check_meas(sub)) {
          clb->call(sub);
        }
      }
    }
  }
  end = true;
}

bool InnerReader::check_meas(const Meas &m) const {
  using utils::inInterval;

  if (m.inFlag(_flag) && (m.inInterval(_from, _to))) {
    return true;
  }
  return false;
}

Reader_ptr InnerReader::clone() const {
  auto res = std::make_shared<InnerReader>(_flag, _from, _to);
  res->_values = _values;
  res->_flag = _flag;
  res->_from = _from;
  res->_to = _to;
  res->_ids = _ids;
  res->end = end;
  res->reset();
  return res;
}
void InnerReader::reset() {
  end = false;
}

size_t InnerReader::size() {
	return _values.size();
}

TP_Reader::TP_Reader() {
  _values_iterator = this->_values.end();
}

TP_Reader::~TP_Reader() {}

bool TP_Reader::isEnd() const {
  return _values_iterator == _values.end();
}

dariadb::IdArray TP_Reader::getIds() const {
  return _ids;
}

void TP_Reader::readNext(dariadb::storage::ReaderClb *clb) {
  if (_values_iterator != _values.end()) {
    clb->call(*_values_iterator);
    ++_values_iterator;
    return;
  }
}

Reader_ptr TP_Reader::clone() const {
  TP_Reader *raw = new TP_Reader;
  raw->_values = _values;
  raw->_ids = _ids;
  raw->reset();
  return Reader_ptr(raw);
}

void TP_Reader::reset() {
  _values_iterator = _values.begin();
}

size_t TP_Reader::size() {
	return this->_values.size();
}