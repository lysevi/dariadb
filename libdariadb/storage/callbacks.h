#pragma once

#include "../meas.h"
#include "../utils/locker.h"
#include <memory>

namespace dariadb {
namespace storage {

class IReaderClb {
public:
  virtual void call(const Meas &m) = 0; // must be thread safety.
  virtual ~IReaderClb() {}
};

typedef std::shared_ptr<IReaderClb> ReaderClb_ptr;

class MList_ReaderClb : public IReaderClb {
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
