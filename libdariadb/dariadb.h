#pragma once

#include <libdariadb/interfaces/iengine.h>
#include <libdariadb/st_exports.h>
#include <string>
namespace dariadb {
/**
autodetect engine type in folder and return instance.
*/
EXPORT IEngine_Ptr open_storage(const std::string &path);
};