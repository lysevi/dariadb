#pragma once

#include "../storage.h"
#include "../utils/locker.h"
#include "chunk.h"

#include <memory>

namespace dariadb {
namespace storage {

class TP_Reader : public storage::Reader {
public:
  TP_Reader();
  ~TP_Reader();

  bool isEnd() const override;

  dariadb::IdArray getIds() const override;

  void readNext(dariadb::storage::ReaderClb *clb) override;

  Reader_ptr clone() const override;

  void reset() override;
  size_t size() override;
  dariadb::Meas::MeasList _values;
  dariadb::Meas::MeasList::iterator _values_iterator;
  dariadb::IdArray _ids;
};
}
}
