#pragma once

#include <libdariadb/meas.h>
#include <libdariadb/st_exports.h>
#include <libdariadb/status.h>
#include <libdariadb/query_param.h>
#include <memory>

namespace dariadb {
class IMeasWriter {
public:
  EXPORT virtual Status append(const Meas &value);
  EXPORT virtual void flush();
  EXPORT virtual Status append(const MeasArray::const_iterator &begin,
                               const MeasArray::const_iterator &end);
  EXPORT virtual Status append(const MeasList::const_iterator &begin,
                               const MeasList::const_iterator &end);
  EXPORT virtual ~IMeasWriter();
};

typedef std::shared_ptr<IMeasWriter> IMeasWriter_ptr;

}
