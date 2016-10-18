#pragma once

#include <libdariadb/append_result.h>
#include <libdariadb/meas.h>
#include <libdariadb/storage/query_param.h>
#include <libdariadb/st_exports.h>
#include <memory>

namespace dariadb {
namespace storage {

class IMeasWriter {
public:
  EXPORT virtual append_result append(const Meas &value);
  EXPORT virtual void flush();
  EXPORT virtual append_result append(const MeasArray::const_iterator &begin,
                               const MeasArray::const_iterator &end);
  EXPORT virtual append_result append(const MeasList::const_iterator &begin,
                               const MeasList::const_iterator &end);
  EXPORT virtual ~IMeasWriter();
};

typedef std::shared_ptr<IMeasWriter> IMeasWriter_ptr;
}
}
