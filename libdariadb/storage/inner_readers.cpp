#include "inner_readers.h"
#include "../flags.h"
#include <cassert>

using namespace dariadb;
using namespace dariadb::compression;
using namespace dariadb::storage;

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
