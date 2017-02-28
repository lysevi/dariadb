#pragma once

#include <libdariadb/interfaces/imeassource.h>
#include <libdariadb/interfaces/imeaswriter.h>
#include <libdariadb/st_exports.h>
#include <libdariadb/utils/utils.h>

namespace dariadb {

class IMeasStorage : public utils::NonCopy, public IMeasSource, public IMeasWriter {
public:
  EXPORT ~IMeasStorage();
};

typedef std::shared_ptr<IMeasStorage> IMeasStorage_ptr;
}
