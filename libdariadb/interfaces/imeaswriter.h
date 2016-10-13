#pragma once

#include <libdariadb/append_result.h>
#include <libdariadb/meas.h>
#include <libdariadb/storage/query_param.h>
#include <memory>

namespace dariadb {
namespace storage {

class IMeasWriter {
public:
  virtual append_result append(const Meas &value) = 0;
  virtual append_result append(const MeasArray::const_iterator &begin,
                               const MeasArray::const_iterator &end);
  virtual append_result append(const MeasList::const_iterator &begin,
                               const MeasList::const_iterator &end);

  virtual void flush() = 0;
  virtual ~IMeasWriter() {}
};

typedef std::shared_ptr<IMeasWriter> IMeasWriter_ptr;
}
}
