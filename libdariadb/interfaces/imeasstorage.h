#pragma once

#include <libdariadb/utils/utils.h>
#include <libdariadb/interfaces/imeassource.h>
#include <libdariadb/interfaces/imeaswriter.h>
#include <libdariadb/st_exports.h>

namespace dariadb {
namespace storage {

class IMeasStorage : public utils::NonCopy, public IMeasSource, public IMeasWriter {
public:
    EXPORT ~IMeasStorage();
};

typedef std::shared_ptr<IMeasStorage> IMeasStorage_ptr;
}
}
