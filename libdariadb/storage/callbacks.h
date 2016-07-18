#pragma once

#include "../meas.h"
#include "../utils/locker.h"
#include <memory>

namespace dariadb {
namespace storage {

class ReaderClb {
public:
  virtual void call(const Meas &m) = 0; // must be thread safety.
  virtual ~ReaderClb() {}
};

typedef std::shared_ptr<ReaderClb> ReaderClb_ptr;

class MList_ReaderClb : public ReaderClb {
public:
  MList_ReaderClb() : mlist() {}
  void call(const Meas &m) override {
    std::lock_guard<utils::Locker> lg(_locker);
    mlist.push_back(m);
  }
  Meas::MeasList mlist;
  utils::Locker _locker;
};
}
}
