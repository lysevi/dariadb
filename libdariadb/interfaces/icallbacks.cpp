#include <libdariadb/interfaces/icallbacks.h>
#include <thread>
using namespace dariadb;
using namespace dariadb::storage;

IReaderClb::IReaderClb() {
  is_end_called = false;
  is_cancel = false;
}

IReaderClb::~IReaderClb() {}

void IReaderClb::is_end() {
  is_end_called = true;
}

void IReaderClb::wait() {
  // TODO make more smarter.
  while (!is_cancel && !is_end_called) {
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
  }
  // mtx.unlock();
}

void IReaderClb::cancel() {
  is_cancel = true;
}

bool IReaderClb::is_canceled() const {
  return is_cancel;
}