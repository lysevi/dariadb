#include <libdariadb/storage/callbacks.h>

using namespace dariadb;
using namespace dariadb::storage;

MList_ReaderClb::MList_ReaderClb() : mlist() {
  is_end_called = false;
}

void MList_ReaderClb::call(const Meas &m) {
  std::lock_guard<utils::Locker> lg(_locker);
  mlist.push_back(m);
}

void MList_ReaderClb::is_end() {
  is_end_called = true;
  _cond.notify_all();
}

void MList_ReaderClb::wait() {
  std::mutex mtx;
  std::unique_lock<std::mutex> locker(mtx);
  _cond.wait(locker, [this]() { return this->is_end_called; });
  //mtx.unlock();
}