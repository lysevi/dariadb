#pragma once

#include <libdariadb/append_result.h>
#include <libdariadb/meas.h>
#include <libdariadb/storage/query_param.h>
#include <libdariadb/dariadb_st_exports.h>
#include <memory>

namespace dariadb {
namespace storage {

class IMeasWriter {
public:
  DARIADB_ST_EXPORTS virtual append_result append(const Meas &value);
  DARIADB_ST_EXPORTS virtual void flush();
  DARIADB_ST_EXPORTS virtual append_result append(const MeasArray::const_iterator &begin,
                               const MeasArray::const_iterator &end);
  DARIADB_ST_EXPORTS virtual append_result append(const MeasList::const_iterator &begin,
                               const MeasList::const_iterator &end);
  DARIADB_ST_EXPORTS virtual ~IMeasWriter();
};

typedef std::shared_ptr<IMeasWriter> IMeasWriter_ptr;
}
}
