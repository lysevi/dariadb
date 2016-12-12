#pragma once

#include <libdariadb/interfaces/imeasstorage.h>
#include <libdariadb/st_exports.h>
#include <extern/libsqlite3/sqlite3.h>



namespace dariadb {
namespace storage {
struct IOAdapter {
  EXPORT IOAdapter(const std::string &fname);
  EXPORT ~IOAdapter();
  EXPORT void stop();
  EXPORT void init_tables();
  sqlite3 *_db;
};
}
}