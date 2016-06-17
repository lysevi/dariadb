#include "aofile.h"
#include "../utils/fs.h"
#include <cassert>
#include <cstring>
#include <fstream>
#include <mutex>

using namespace dariadb;
using namespace dariadb::storage;

class AOFile::Private {
public:
  Private(const AOFile::Params &params) : _params(params), mmap(nullptr) {
    _is_readonly = false;
    create();
  }

  Private(const AOFile::Params &params, const std::string &fname, bool readonly)
      : _params(params), mmap(nullptr) {
    _is_readonly = readonly;
    open(fname);
  }

  ~Private() {
    this->flush();
    if (!_is_readonly) {
      if (mmap != nullptr) {
        _header->is_closed = true;
        mmap->close();
      }
    }
  }

  void create() { load(); }

  void load() {}

  void open(const std::string &fname) {}

  void restore() {}

  append_result append(const Meas &value) {
    assert(!_is_readonly);
    std::lock_guard<std::mutex> lock(_mutex);
    return append_result(0, 1);
  }

  Reader_ptr readInterval(const QueryInterval &q) {
    std::lock_guard<std::mutex> lock(_mutex);

    return nullptr;
  }

  Reader_ptr readInTimePoint(const QueryTimePoint &q) {
    std::lock_guard<std::mutex> lock(_mutex);
    return nullptr;
  }

  Reader_ptr currentValue(const IdArray &ids, const Flag &flag) {
    std::lock_guard<std::mutex> lock(_mutex);
    return readInTimePoint(QueryTimePoint(ids, flag, this->maxTime()));
  }

  dariadb::Time minTime() const {
    std::lock_guard<std::mutex> lock(_mutex);
    return _header->minTime;
  }

  dariadb::Time maxTime() const {
    std::lock_guard<std::mutex> lock(_mutex);
    return _header->maxTime;
  }

  bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                  dariadb::Time *maxResult) {
    std::lock_guard<std::mutex> lock(_mutex);
    return false;
  }

  void flush() {
    std::lock_guard<std::mutex> lock(_mutex);
    // if (drop_future.valid()) {
    //  drop_future.wait();
    //}
    mmap->flush();
  }

  void drop_to_stor(MeasWriter *stor) { NOT_IMPLEMENTED; }

protected:
  AOFile::Params _params;

  dariadb::utils::fs::MappedFile::MapperFile_ptr mmap;
  AOFile::Header *_header;
  uint8_t *_raw_data;

  mutable std::mutex _mutex;
  bool _is_readonly;
};

AOFile::~AOFile() {}

AOFile::AOFile(const Params &params) : _Impl(new AOFile::Private(params)) {}

AOFile::AOFile(const AOFile::Params &params, const std::string &fname,
               bool readonly)
    : _Impl(new AOFile::Private(params, fname, readonly)) {}

AOFile::Header AOFile::readHeader(std::string file_name) {
  std::ifstream istream;
  istream.open(file_name, std::fstream::in | std::fstream::binary);
  if (!istream.is_open()) {
    std::stringstream ss;
    ss << "can't open file. filename=" << file_name;
    throw MAKE_EXCEPTION(ss.str());
  }
  AOFile::Header result;
  memset(&result, 0, sizeof(AOFile::Header));
  istream.read((char *)&result, sizeof(AOFile::Header));
  istream.close();
  return result;
}
dariadb::Time AOFile::minTime() { return _Impl->minTime(); }

dariadb::Time AOFile::maxTime() { return _Impl->maxTime(); }

bool AOFile::minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                        dariadb::Time *maxResult) {
  return _Impl->minMaxTime(id, minResult, maxResult);
}
void AOFile::flush() { // write all to storage;
  _Impl->flush();
}

append_result AOFile::append(const Meas &value) { return _Impl->append(value); }

Reader_ptr AOFile::readInterval(const QueryInterval &q) {
  return _Impl->readInterval(q);
}

Reader_ptr AOFile::readInTimePoint(const QueryTimePoint &q) {
  return _Impl->readInTimePoint(q);
}

Reader_ptr AOFile::currentValue(const IdArray &ids, const Flag &flag) {
  return _Impl->currentValue(ids, flag);
}

void AOFile::drop_to_stor(MeasWriter *stor) { _Impl->drop_to_stor(stor); }
