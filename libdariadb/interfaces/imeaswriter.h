#pragma once

#include <libdariadb/meas.h>
#include <libdariadb/query.h>
#include <libdariadb/st_exports.h>
#include <libdariadb/status.h>
#include <memory>

namespace dariadb {
class IMeasWriter {
public:
  EXPORT Status append(Id i, Time t, Flag f, Value v) {
    Meas m;
    m.id = i;
    m.time = t;
    m.flag = f;
    m.value = v;
    return append(m);
  }

  EXPORT Status append(Id i, Time t, Value v) { return append(i, t, Flag(), v); }

  EXPORT virtual Status append(const Meas &value) = 0;
  EXPORT virtual void flush();
  EXPORT virtual void flush(Id id);
  EXPORT virtual Status append(const MeasArray::const_iterator &begin,
                               const MeasArray::const_iterator &end);
  EXPORT virtual ~IMeasWriter();
};

typedef std::shared_ptr<IMeasWriter> IMeasWriter_ptr;
} // namespace dariadb
