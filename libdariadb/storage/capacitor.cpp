#include "capacitor.h"
#include "../timeutil.h"
#include "../utils/locker.h"
#include "../utils/utils.h"
#include "../utils/fs.h"
#include <algorithm>
#include <cassert>
#include <limits>
#include <list>
#include <map>
#include <unordered_map>
#include <utility>

using namespace dariadb;
using namespace dariadb::storage;

class Capacitor::Private {
public:
  struct Header {
	  bool is_dropped;
	  size_t B;
	  size_t size;
  };

  Private(const BaseStorage_ptr stor, const Capacitor::Params &params)
      : _minTime(std::numeric_limits<dariadb::Time>::max()),
        _maxTime(std::numeric_limits<dariadb::Time>::min()), _stor(stor),
        _params(params), mmap(nullptr) {
	  
	  open_or_create();
  }

  
  ~Private() {
	  if (mmap != nullptr) {
		  mmap->close();
		  
	  }
	  
  }

  void open_or_create() {
	  if (!dariadb::utils::fs::path_exists(_params.path)) {
		  create();
	  }
	  else {
		  open();
	  }
  }

  uint64_t cap_size()const {
    return (1024 * 1024) / sizeof(Meas) + sizeof(Header);
  }

  std::string file_name() {
	  return "1" + CAP_FILE_EXT;
  }

  void create() {
	  dariadb::utils::fs::mkdir(_params.path);
	  utils::fs::mkdir(_params.path);
      mmap = utils::fs::MappedFile::touch(
              utils::fs::append_path(_params.path, file_name()),
              cap_size());

  }

  void open() {
    mmap = utils::fs::MappedFile::open(
        utils::fs::append_path(_params.path, file_name()));
  }

  append_result append(const Meas &value) { return append_result(0, 0); }

  Reader_ptr readInterval(Time from, Time to) { return nullptr; }
  virtual Reader_ptr readInTimePoint(Time time_point) { return nullptr; }

  virtual Reader_ptr readInterval(const IdArray &ids, Flag flag, Time from,
                                  Time to) {
    return nullptr;
  }

  virtual Reader_ptr readInTimePoint(const IdArray &ids, Flag flag,
                                     Time time_point) {
    return nullptr;
  }

  dariadb::Time minTime() const { return _minTime; }
  dariadb::Time maxTime() const { return _maxTime; }

  bool flush() { return false; }
  size_t in_queue_size() const { return 0; }

protected:
  dariadb::Time _minTime;
  dariadb::Time _maxTime;
  BaseStorage_ptr _stor;
  Capacitor::Params _params;

  dariadb::utils::fs::MappedFile::MapperFile_ptr mmap;
};

Capacitor::~Capacitor() {}

Capacitor::Capacitor(const BaseStorage_ptr stor, const Params &params)
    : _Impl(new Capacitor::Private(stor, params)) {}

dariadb::Time Capacitor::minTime() {
  return _Impl->minTime();
}

dariadb::Time Capacitor::maxTime() {
  return _Impl->maxTime();
}

bool Capacitor::flush() { // write all to storage;
  return _Impl->flush();
}

append_result dariadb::storage::Capacitor::append(const Meas &value) {
  return _Impl->append(value);
}

Reader_ptr dariadb::storage::Capacitor::readInterval(Time from, Time to) {
  return _Impl->readInterval(from, to);
}

Reader_ptr dariadb::storage::Capacitor::readInTimePoint(Time time_point) {
  return _Impl->readInTimePoint(time_point);
}

Reader_ptr dariadb::storage::Capacitor::readInterval(const IdArray &ids,
                                                     Flag flag, Time from,
                                                     Time to) {
  return _Impl->readInterval(ids, flag, from, to);
}

Reader_ptr dariadb::storage::Capacitor::readInTimePoint(const IdArray &ids,
                                                        Flag flag,
                                                        Time time_point) {
  return _Impl->readInTimePoint(ids, flag, time_point);
}

size_t dariadb::storage::Capacitor::in_queue_size() const {
  return _Impl->in_queue_size();
}