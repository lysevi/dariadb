#include <libdariadb/storage/manifest.h>
#include <libdariadb/utils/exception.h>
#include <libdariadb/utils/fs.h>
#include <algorithm>
#include <cassert>
#include <fstream>
#include <json/src/json.hpp>

using json = nlohmann::json;
using namespace dariadb::storage;

const std::string PAGE_JS_KEY = "pages";
const std::string AOF_JS_KEY = "aofs";

const char* CREATE_SQL =
"CREATE TABLE IF NOT EXISTS pages(id INTEGER PRIMARY KEY AUTOINCREMENT, file varchar(255)); "\
"CREATE TABLE IF NOT EXISTS aofs(id INTEGER PRIMARY KEY AUTOINCREMENT, file varchar(255)); "\
"CREATE TABLE IF NOT EXISTS params(id INTEGER PRIMARY KEY AUTOINCREMENT, name varchar(255), value varchar(255)); "\
	;

static int file_select_callback(void *data, int argc, char **argv, char **azColName) {
	std::list<std::string> *ld = (std::list<std::string>*)data;
	assert(argc == 1);
	ld->push_back (std::string(argv[0]));
	return 0;
}

static int version_select_callback(void *data, int argc, char **argv, char **azColName) {
	std::string *ld = (std::string*)data;
	assert(argc == 1);
	(*ld) = std::string(argv[0]);
	return 0;
}

Manifest::Manifest(const std::string &fname) : _filename(fname) {
	std::string storage_path = utils::fs::parent_path(this->_filename);
	bool is_exists = true;
	if (!utils::fs::path_exists(storage_path)) {
		try {
			is_exists = false;
			utils::fs::mkdir(storage_path);
		}
		catch (std::exception&ex) {
			auto msg = ex.what();
			THROW_EXCEPTION("Can't create folder: ", msg);
		}
	}
  int rc = sqlite3_open(fname.c_str(), &db);
  if (rc) {
	  auto err_msg = sqlite3_errmsg(db);
    THROW_EXCEPTION("Can't open database: ", err_msg);
  } 
  char *err = 0;
  if (sqlite3_exec(db, CREATE_SQL, 0, 0, &err)){
	  fprintf(stderr, "Îøèáêà SQL: %sn", err);
	  sqlite3_free(err);
  }
  if (is_exists) {
	  restore();
  }
}

Manifest::~Manifest() {
  sqlite3_close(db);
}

void Manifest::restore() {
  std::string storage_path = utils::fs::parent_path(this->_filename);

  auto aofs = this->aof_list();
  auto size_before = aofs.size();
  aofs.erase(std::remove_if(aofs.begin(), aofs.end(),
                            [this, storage_path](std::string fname) {
                              auto full_file_name =
                                  utils::fs::append_path(storage_path, fname);
                              return !utils::fs::path_exists(full_file_name);
                            }),
             aofs.end());
  auto size_after = aofs.size();
  if (size_after != size_before) {
	  clear_field_values(AOF_JS_KEY);
	  for (auto fname : aofs) {
		  this->aof_append(fname);
	  }
  }

  auto pages = this->page_list();
  size_before = pages.size();
  pages.erase(std::remove_if(pages.begin(), pages.end(),
                             [this, storage_path](std::string fname) {
                               auto full_file_name =
                                   utils::fs::append_path(storage_path, fname);
                               return !utils::fs::path_exists(full_file_name);
                             }),
              pages.end());
  size_after = pages.size();
  if (size_after != size_before) {
	  clear_field_values(PAGE_JS_KEY);
	  for (auto fname : pages) {
		  this->page_append(fname);
	  }
  }
}

std::list<std::string> Manifest::page_list() {
  std::lock_guard<utils::Locker> lg(_locker);
  std::string sql = "SELECT file from pages;";
  std::list<std::string> result{};
  char *zErrMsg = 0;
  auto rc = sqlite3_exec(db, sql.c_str(), file_select_callback, (void*)&result, &zErrMsg);
  if (rc != SQLITE_OK) {
	  THROW_EXCEPTION("engine: SQL error - %s\n", zErrMsg);
	  sqlite3_free(zErrMsg);
  }

  return result;
}

void Manifest::page_append(const std::string &rec) {
  std::lock_guard<utils::Locker> lg(_locker);

  std::stringstream ss;
  ss << "insert into pages (file) values ('" << rec << "');";
  auto sql = ss.str();
  std::list<std::string> result{};
  char *zErrMsg = 0;
  auto rc = sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &zErrMsg);
  if (rc != SQLITE_OK) {
	  THROW_EXCEPTION("engine: SQL error - %s\n", zErrMsg);
	  sqlite3_free(zErrMsg);
  }
}

void Manifest::page_rm(const std::string &rec) {
  std::lock_guard<utils::Locker> lg(_locker);

  std::stringstream ss;
  ss << "delete from pages where file = '" << rec << "';";
  auto sql = ss.str();
  std::list<std::string> result{};
  char *zErrMsg = 0;
  auto rc = sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &zErrMsg);
  if (rc != SQLITE_OK) {
	  THROW_EXCEPTION("engine: SQL error - %s\n", zErrMsg);
	  sqlite3_free(zErrMsg);
  }
}

std::list<std::string> Manifest::aof_list() {
	std::lock_guard<utils::Locker> lg(_locker);
	std::string sql = "SELECT file from aofs;";
	std::list<std::string> result{};
	char *zErrMsg = 0;
	auto rc = sqlite3_exec(db, sql.c_str(), file_select_callback, (void*)&result, &zErrMsg);
	if (rc != SQLITE_OK) {
		THROW_EXCEPTION("engine: SQL error - %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}
	return result;
}

void Manifest::clear_field_values(std::string field_name) {
	std::stringstream ss;
	ss << "delete from "<<field_name << ";";
	auto sql = ss.str();
	std::list<std::string> result{};
	char *zErrMsg = 0;
	auto rc = sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &zErrMsg);
	if (rc != SQLITE_OK) {
		THROW_EXCEPTION("engine: SQL error - %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}
}

void Manifest::aof_append(const std::string &rec) {
  std::lock_guard<utils::Locker> lg(_locker);

  std::stringstream ss;
  ss << "insert into aofs (file) values ('" << rec << "');";
  auto sql = ss.str();
  std::list<std::string> result{};
  char *zErrMsg = 0;
  auto rc = sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &zErrMsg);
  if (rc != SQLITE_OK) {
	  THROW_EXCEPTION("engine: SQL error - %s\n", zErrMsg);
	  sqlite3_free(zErrMsg);
  }
}

void Manifest::aof_rm(const std::string &rec) {
  std::lock_guard<utils::Locker> lg(_locker);

  std::stringstream ss;
  ss << "delete from aofs where file = '" << rec << "';";
  auto sql = ss.str();
  std::list<std::string> result{};
  char *zErrMsg = 0;
  auto rc = sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &zErrMsg);
  if (rc != SQLITE_OK) {
	  THROW_EXCEPTION("engine: SQL error - %s\n", zErrMsg);
	  sqlite3_free(zErrMsg);
  }
}

void Manifest::set_version(const std::string &version) {
  std::lock_guard<utils::Locker> lg(_locker);

  std::stringstream ss;
  ss << "insert or replace into params(id, name, value) values ((select id from params "
        "where name = 'version'), 'version', '"
     << version << "' );";
  auto sql = ss.str();
  std::list<std::string> result{};
  char *zErrMsg = 0;
  auto rc = sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &zErrMsg);
  if (rc != SQLITE_OK) {
    THROW_EXCEPTION("engine: SQL error - %s\n", zErrMsg);
    sqlite3_free(zErrMsg);
  }
}

std::string Manifest::get_version() {
	std::string sql = "SELECT value from params where name='version';";
	std::string result;
	char *zErrMsg = 0;
	auto rc = sqlite3_exec(db, sql.c_str(), version_select_callback, (void*)&result, &zErrMsg);
	if (rc != SQLITE_OK) {
		THROW_EXCEPTION("engine: SQL error - %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}
	return result;
}
