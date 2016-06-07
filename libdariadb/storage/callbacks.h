#pragma once

#include "../meas.h"
#include <cassert>
#include <memory>

namespace dariadb {
namespace storage {

class ReaderClb {
public:
  virtual void call(const Meas &m) = 0;
  virtual ~ReaderClb() {}
};

typedef std::shared_ptr<ReaderClb> ReaderClb_ptr;

class MList_ReaderClb : public ReaderClb {
public:
  void call(const Meas &m) override {
    assert(mlist != nullptr);
    mlist->push_back(m);
  }
  Meas::MeasList *mlist;
};
}
}
