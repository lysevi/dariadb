#include <libdariadb/interfaces/icallbacks.h>
#include <libdariadb/utils/utils.h>
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
  while (!is_end_called) {
    utils::sleep_mls(300);
  }
}

void IReadCallback::cancel() {
  is_cancel = true;
}

bool IReadCallback::is_canceled() const {
  return is_cancel;
}