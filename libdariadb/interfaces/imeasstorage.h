#pragma once

#include "../utils/utils.h"
#include "imeassource.h"
#include "imeaswriter.h"

namespace dariadb {
namespace storage {

class IMeasStorage : public utils::NonCopy, public IMeasSource, public IMeasWriter {
public:
};

typedef std::shared_ptr<IMeasStorage> IMeasStorage_ptr;
}
}
