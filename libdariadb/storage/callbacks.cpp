#include <libdariadb/storage/callbacks.h>

using namespace dariadb;
using namespace dariadb::storage;

MList_ReaderClb::MList_ReaderClb() : mlist() {}

void MList_ReaderClb::apply(const Meas &m) {
  std::lock_guard<utils::async::Locker> lg(_locker);
  mlist.push_back(m);
}
