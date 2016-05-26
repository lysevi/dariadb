#include "inner_readers.h"
#include "../flags.h"
#include <cassert>

using namespace dariadb;
using namespace dariadb::compression;
using namespace dariadb::storage;

class CursorReader : public dariadb::storage::Cursor::Callback {
public:
  Chunk_Ptr readed;
  CursorReader() { readed = nullptr; }
  void call(dariadb::storage::Chunk_Ptr &ptr) override { readed = ptr; }
};

InnerReader::InnerReader(dariadb::Flag flag, dariadb::Time from, dariadb::Time to)
    : _cursors{}, _flag(flag), _from(from), _to(to) {
  end = false;
}

void InnerReader::add(Cursor_ptr c) {
  std::lock_guard<std::mutex> lg(_locker);
  this->_cursors.push_back(c);
}

bool InnerReader::isEnd() const {
  return this->end;
}

dariadb::IdArray InnerReader::getIds() const {
  return _ids;
}

void InnerReader::readNext(storage::ReaderClb *clb) {
  std::lock_guard<std::mutex> lg(_locker);

  std::shared_ptr<CursorReader> reader_clbk{new CursorReader};
  for (auto id : this->_ids) {
    for (auto ch : _cursors) {
      while (!ch->is_end()) {
        ch->readNext(reader_clbk.get());
        if (reader_clbk->readed == nullptr) {
          continue;
        }
        auto cur_ch = reader_clbk->readed;
        reader_clbk->readed = nullptr;
        auto ch_reader = cur_ch->get_reader();
        size_t read_count = 0;
        while (!ch_reader->is_end()) {
          auto sub = ch_reader->readNext();
          read_count++;
          if (sub.id == id) {
            if (check_meas(sub)) {
              clb->call(sub);
            }
          }
        }
      }
    }
  }
  end = true;
}

bool InnerReader::check_meas(const Meas &m) const {
  auto tmp = std::make_tuple(m.id, m.time);
  
  using utils::inInterval;

  if (m.inFlag(_flag) && (m.inInterval(_from, _to))) {
    return true;
  }
  return false;
}

Reader_ptr InnerReader::clone() const {
  auto res = std::make_shared<InnerReader>(_flag, _from, _to);
  res->_cursors = _cursors;
  res->_flag = _flag;
  res->_from = _from;
  res->_to = _to;
  res->end = end;
  return res;
}
void InnerReader::reset() {
  end = false;
  for (auto ch : _cursors) {
    ch->reset_pos();
  }
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
  raw->reset();
  return Reader_ptr(raw);
}

void TP_Reader::reset() {
  _values_iterator = _values.begin();
}
