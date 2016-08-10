#pragma once

#include "../common.h"
#include "../meas.h"
#include "../storage/query_param.h"
#include <memory>

namespace dariadb {
namespace storage {

class IMeasWriter {
public:
  virtual append_result append(const Meas &value) = 0;
  virtual append_result append(const Meas::MeasArray::const_iterator &begin,
                               const Meas::MeasArray::const_iterator &end);
  virtual append_result append(const Meas::MeasList::const_iterator &begin,
                               const Meas::MeasList::const_iterator &end);

  virtual void flush() = 0;
  virtual ~IMeasWriter() {}
};

typedef std::shared_ptr<IMeasWriter> IMeasWriter_ptr;
}
}
