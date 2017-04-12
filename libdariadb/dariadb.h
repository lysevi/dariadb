#pragma once

#include <libdariadb/engines/engine.h>
#include <libdariadb/engines/shard.h>
#include <libdariadb/interfaces/iengine.h>
#include <libdariadb/scheme/scheme.h>
#include <libdariadb/st_exports.h>
#include <libdariadb/storage/callbacks.h>
#include <libdariadb/storage/settings.h>
#include <libdariadb/timeutil.h>

#include <string>

namespace dariadb {
/**
autodetect engine type in folder and return instance.
*/
EXPORT IEngine_Ptr open_storage(const std::string &path);
}
