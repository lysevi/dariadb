#pragma once

#include <libdariadb/interfaces/icallbacks.h>
#include <libdariadb/meas.h>
#include <libdariadb/utils/locker.h>
#include <memory>

namespace dariadb {
namespace storage {

class MList_ReaderClb : public IReaderClb {
public:
  MList_ReaderClb() : mlist() { is_end_called = false; }
  void call(const Meas &m) override {
    std::lock_guard<utils::Locker> lg(_locker);
    mlist.push_back(m);
  }
  void is_end() override { is_end_called = true; }
  MeasList mlist;
  utils::Locker _locker;
  bool is_end_called;
};
}
}
