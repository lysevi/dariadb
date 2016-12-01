#include <libdariadb/storage/callbacks.h>

using namespace dariadb;
using namespace dariadb::storage;

MList_ReaderClb::MList_ReaderClb() : mlist() {
}

void MList_ReaderClb::call(const Meas &m) {
  std::lock_guard<utils::Locker> lg(_locker);
  mlist.push_back(m);
}
