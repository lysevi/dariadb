#include <libdariadb/storage/bystep/io_adapter.h>
#include <libdariadb/utils/logger.h>

using namespace dariadb;
using namespace dariadb::storage;

const char *BYSTEP_CREATE_SQL =
"CREATE TABLE IF NOT EXISTS Meases(id INTEGER PRIMARY KEY "
"AUTOINCREMENT, chunk blob); ";

IOAdapter::IOAdapter(sqlite3 *db) {
  _db = db;
  _db_owner = false;
  init_tables();
}

IOAdapter::IOAdapter(const std::string &fname) {
	_db = nullptr;
	int rc = sqlite3_open(fname.c_str(), &_db);
	if (rc) {
		auto err_msg = sqlite3_errmsg(_db);
		THROW_EXCEPTION("Can't open database: ", err_msg);
	}
	_db_owner = true;
	init_tables();
}

IOAdapter::~IOAdapter() {
	if (_db_owner) {
		sqlite3_close(_db);
	}
}

void IOAdapter::init_tables() {
  logger_info("engine: io_adapter - init_tables");
  char *err = 0;
  if (sqlite3_exec(_db, BYSTEP_CREATE_SQL, 0, 0, &err)) {
    auto err_msg = sqlite3_errmsg(_db);
    sqlite3_free(err);
    THROW_EXCEPTION("Can't init database: ", err_msg);
  }
}