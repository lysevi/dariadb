#include <libdariadb/storage/manifest.h>
#include <libdariadb/utils/exception.h>
#include <libdariadb/utils/fs.h>
#include <libsqlite3/sqlite3.h>
#include <algorithm>
#include <cassert>
#include <fstream>

using namespace dariadb;
using namespace dariadb::storage;

const std::string PAGE_JS_KEY = "pages";
const std::string AOF_JS_KEY = "aofs";

const char *MANIFEST_CREATE_SQL = 
"CREATE TABLE IF NOT EXISTS pages(id INTEGER PRIMARY KEY AUTOINCREMENT, file varchar(255)); "
"CREATE TABLE IF NOT EXISTS aofs(id INTEGER PRIMARY KEY AUTOINCREMENT, file varchar(255)); "
"CREATE TABLE IF NOT EXISTS params(id INTEGER PRIMARY KEY AUTOINCREMENT, name varchar(255), value varchar(255)); "
"CREATE TABLE IF NOT EXISTS id2step(bystep_id INTEGER UNIQUE PRIMARY KEY, step varchar(50)); ";

static int file_select_callback(void *data, int argc, char **argv, char **azColName) {
  std::list<std::string> *ld = (std::list<std::string> *)data;
  assert(argc == 1);
  ld->push_back(std::string(argv[0]));
  return 0;
}

static int version_select_callback(void *data, int argc, char **argv, char **azColName) {
  std::string *ld = (std::string *)data;
  assert(argc == 1);
  (*ld) = std::string(argv[0]);
  return 0;
}

class Manifest::Private {
public:
  Private(const std::string &fname) : _filename(fname) {
    std::string storage_path = utils::fs::parent_path(this->_filename);
    bool is_exists = true;
    if (!utils::fs::path_exists(storage_path)) {
      try {
        is_exists = false;
        utils::fs::mkdir(storage_path);
      } catch (std::exception &ex) {
        auto msg = ex.what();
        THROW_EXCEPTION("Can't create folder: ", msg);
      }
    }
	logger_info("engine: opening  manifest file...");
    int rc = sqlite3_open(fname.c_str(), &db);
    if (rc) {
      auto err_msg = sqlite3_errmsg(db);
      THROW_EXCEPTION("Can't open database: ", err_msg);
    }
	logger_info("engine: manifest file opened.");
    char *err = 0;
    if (sqlite3_exec(db, MANIFEST_CREATE_SQL, 0, 0, &err)) {
      fprintf(stderr, "Îøèáêà SQL: %sn", err);
      sqlite3_free(err);
    }
    if (is_exists) {
      restore();
    }
  }

  ~Private() { sqlite3_close(db); }

  void restore() {
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

  std::list<std::string> page_list() {
    std::lock_guard<utils::async::Locker> lg(_locker);
    std::string sql = "SELECT file from pages;";
    std::list<std::string> result{};
    char *zErrMsg = 0;
    auto rc =
        sqlite3_exec(db, sql.c_str(), file_select_callback, (void *)&result, &zErrMsg);
    if (rc != SQLITE_OK) {
		std::string msg = std::string(zErrMsg);
		sqlite3_free(zErrMsg);
      THROW_EXCEPTION("engine: SQL error - %s\n", msg);
    }

    return result;
  }

  void page_append(const std::string &rec) {
    std::lock_guard<utils::async::Locker> lg(_locker);

    std::stringstream ss;
    ss << "insert into pages (file) values ('" << rec << "');";
    auto sql = ss.str();
    char *zErrMsg = 0;
    auto rc = sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &zErrMsg);
    if (rc != SQLITE_OK) {
		std::string msg = std::string(zErrMsg);
		sqlite3_free(zErrMsg);
		THROW_EXCEPTION("engine: SQL error - %s\n", msg);
    }
  }

  void page_rm(const std::string &rec) {
    std::lock_guard<utils::async::Locker> lg(_locker);

    std::stringstream ss;
    ss << "delete from pages where file = '" << rec << "';";
    auto sql = ss.str();
    char *zErrMsg = 0;
    auto rc = sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &zErrMsg);
    if (rc != SQLITE_OK) {
		std::string msg = std::string(zErrMsg);
		sqlite3_free(zErrMsg);
		THROW_EXCEPTION("engine: SQL error - %s\n", msg);
    }
  }

  std::list<std::string> aof_list() {
    std::lock_guard<utils::async::Locker> lg(_locker);
    std::string sql = "SELECT file from aofs;";
    std::list<std::string> result{};
    char *zErrMsg = 0;
    auto rc =
        sqlite3_exec(db, sql.c_str(), file_select_callback, (void *)&result, &zErrMsg);
    if (rc != SQLITE_OK) {
		std::string msg = std::string(zErrMsg);
		sqlite3_free(zErrMsg);
		THROW_EXCEPTION("engine: SQL error - %s\n", msg);
    }
    return result;
  }

  void clear_field_values(const std::string &field_name) {
    std::stringstream ss;
    ss << "delete from " << field_name << ";";
    auto sql = ss.str();
    char *zErrMsg = 0;
    auto rc = sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &zErrMsg);
    if (rc != SQLITE_OK) {
		std::string msg = std::string(zErrMsg);
		sqlite3_free(zErrMsg);
		THROW_EXCEPTION("engine: SQL error - %s\n", msg);
    }
  }

  void aof_append(const std::string &rec) {
    std::lock_guard<utils::async::Locker> lg(_locker);

    std::stringstream ss;
    ss << "insert into aofs (file) values ('" << rec << "');";
    auto sql = ss.str();
    char *zErrMsg = 0;
    auto rc = sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &zErrMsg);
    if (rc != SQLITE_OK) {
		std::string msg = std::string(zErrMsg);
		sqlite3_free(zErrMsg);
		THROW_EXCEPTION("engine: SQL error - %s\n", msg);
    }
  }

  void aof_rm(const std::string &rec) {
    std::lock_guard<utils::async::Locker> lg(_locker);

    std::stringstream ss;
    ss << "delete from aofs where file = '" << rec << "';";
    auto sql = ss.str();
    char *zErrMsg = 0;
    auto rc = sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &zErrMsg);
    if (rc != SQLITE_OK) {
		std::string msg = std::string(zErrMsg);
		sqlite3_free(zErrMsg);
		THROW_EXCEPTION("engine: SQL error - %s\n", msg);
    }
  }

  void set_version(const std::string &version) {
    std::lock_guard<utils::async::Locker> lg(_locker);

    std::stringstream ss;
    ss << "insert or replace into params(id, name, value) values ((select id from params "
          "where name = 'version'), 'version', '"
       << version << "' );";
    auto sql = ss.str();
    char *zErrMsg = 0;
    auto rc = sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &zErrMsg);
    if (rc != SQLITE_OK) {
		std::string msg = std::string(zErrMsg);
		sqlite3_free(zErrMsg);
		THROW_EXCEPTION("engine: SQL error - %s\n", msg);
    }
  }

  std::string get_version() {
    std::string sql = "SELECT value from params where name='version';";
    std::string result;
    char *zErrMsg = 0;
    auto rc =
        sqlite3_exec(db, sql.c_str(), version_select_callback, (void *)&result, &zErrMsg);
    if (rc != SQLITE_OK) {
		std::string msg = std::string(zErrMsg);
		sqlite3_free(zErrMsg);
		THROW_EXCEPTION("engine: SQL error - %s\n", msg);
    }
    return result;
  }

  void insert_id2step(const Id2Step&i2s) {
	  sqlite3_exec(db, "BEGIN TRANSACTION;", NULL, NULL, NULL);

	  
	  try {
		  for (auto&kv : i2s) {
			  const std::string sql_query =
				  "INSERT INTO id2step(bystep_id, step) values (?,?);";
			  sqlite3_stmt *pStmt;
			  int rc;
			  do {
				  rc = sqlite3_prepare(db, sql_query.c_str(), -1, &pStmt, 0);
				  if (rc != SQLITE_OK) {
					  auto err_msg = std::string(sqlite3_errmsg(db));
					  THROW_EXCEPTION("engine: Manifest - ", err_msg);
				  }
				 
				  std::stringstream ss;
				  ss << kv.second;
				  auto str_step = ss.str();
				  sqlite3_bind_int64(pStmt, 1, kv.first);
				  sqlite3_bind_text(pStmt, 2, str_step.c_str(), str_step.size(), SQLITE_STATIC);

				  rc = sqlite3_step(pStmt);
				  assert(rc != SQLITE_ROW);
				  rc = sqlite3_finalize(pStmt);
			  } while (rc == SQLITE_SCHEMA);
		  }
		  sqlite3_exec(db, "END TRANSACTION;", NULL, NULL, NULL);
	  }
	  catch(...){
		  sqlite3_exec(db, "ROLLBACK TRANSACTION;", NULL, NULL, NULL);
		  throw;
	  }
  }

  Id2Step read_id2step() {
	  Id2Step i2s;
	  
	  sqlite3_stmt *pStmt;
	  int rc;
	  const std::string sql_query = "SELECT bystep_id, step FROM id2step";
	  do {
		  rc = sqlite3_prepare(db, sql_query.c_str(), -1, &pStmt, 0);
		  if (rc != SQLITE_OK) {
			  auto err_msg = std::string(sqlite3_errmsg(db));
			  THROW_EXCEPTION("engine: Manifest - ", err_msg);
		  }


		  while (1) {
			  rc = sqlite3_step(pStmt);
			  if (rc == SQLITE_ROW) {
				  auto bs= sqlite3_column_int64(pStmt, 0);
				  auto step_str =std::string((char*)sqlite3_column_text(pStmt, 1));
				  
				  std::istringstream iss(step_str);
				  STEP_KIND sk;
				  iss>>sk;

				  i2s[bs] = sk;
			  }
			  else {
				  break;
			  }
		  }
		  rc = sqlite3_finalize(pStmt);
	  } while (rc == SQLITE_SCHEMA);
	  return i2s;
  }
protected:
  std::string _filename;
  utils::async::Locker _locker;
  sqlite3 *db;
};


Manifest::Manifest(const std::string &fname):_impl(new Manifest::Private(fname)) {}
Manifest::~Manifest() {
	_impl = nullptr;
}

std::list<std::string> Manifest::page_list() {
	return _impl->page_list();
}

void Manifest::page_append(const std::string &rec) {
	_impl->page_append(rec);
}

void Manifest::page_rm(const std::string &rec) {
	_impl->page_rm(rec);
}

std::list<std::string> Manifest::aof_list() {
	return _impl->aof_list();
}

void Manifest::aof_append(const std::string &rec) {
	_impl->aof_append(rec);
}

void Manifest::aof_rm(const std::string &rec) {
	_impl->aof_rm(rec);
}

void Manifest::set_version(const std::string &version) {
	_impl->set_version(version);
}

std::string Manifest::get_version() {
	return _impl->get_version();
}

void  Manifest::insert_id2step(const Id2Step&i2s) {
	_impl->insert_id2step(i2s);
}

Id2Step Manifest::read_id2step() {
	return _impl->read_id2step();
}