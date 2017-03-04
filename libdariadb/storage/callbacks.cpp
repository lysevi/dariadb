#include <libdariadb/storage/callbacks.h>

using namespace dariadb;
using namespace dariadb::storage;

MArray_ReaderClb::MArray_ReaderClb(size_t count) {
  this->marray.reserve(count);
}

void MArray_ReaderClb::apply(const Meas &m) {
  std::lock_guard<utils::async::Locker> lg(_locker);
  marray.push_back(m);
}

MArrayPtr_ReaderClb::MArrayPtr_ReaderClb(MeasArray *output) {
  this->marray = output;
}

void MArrayPtr_ReaderClb::apply(const Meas &m) {
  std::lock_guard<utils::async::Locker> lg(_locker);
  marray->push_back(m);
}
