#include <libdariadb/interfaces/icallbacks.h>
#include <thread>
using namespace dariadb;

IReadCallback::IReadCallback() {
  is_end_called = false;
  is_cancel = false;
}

IReadCallback::~IReadCallback() {}

void IReadCallback::is_end() {
  is_end_called = true;
}

void IReadCallback::wait() {
  // TODO make more smarter.
  while (!is_cancel && !is_end_called) {
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
  }
  // mtx.unlock();
}

void IReadCallback::cancel() {
  is_cancel = true;
}

bool IReadCallback::is_canceled() const {
  return is_cancel;
}