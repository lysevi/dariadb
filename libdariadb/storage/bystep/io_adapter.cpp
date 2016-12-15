#include <libdariadb/storage/bystep/io_adapter.h>
#include <libdariadb/utils/logger.h>
#include <cassert>
#include <extern/libsqlite3/sqlite3.h>
#include <string>

using namespace dariadb;
using namespace dariadb::storage;

const char *BYSTEP_CREATE_SQL = "CREATE TABLE IF NOT EXISTS Chunks("
                                "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                                "number INTEGER,"  // chunk::id
                                "meas_id INTEGER," // measurement id
                                "min_time INTEGER,"
                                "max_time INTEGER,"
                                "chunk_header blob,"
                                "chunk_buffer blob);";

class IOAdapter::Private {
public:
  Private(const std::string &fname) {
    _db = nullptr;
    logger("engine: io_adapter - open ", fname);
    int rc = sqlite3_open(fname.c_str(), &_db);
    if (rc) {
      auto err_msg = sqlite3_errmsg(_db);
      THROW_EXCEPTION("Can't open database: ", err_msg);
    }
    init_tables();
  }
  ~Private() { stop(); }

  void stop() {
    if (_db != nullptr) {
      sqlite3_close(_db);
      _db = nullptr;
      logger("engine: io_adapter - stoped");
    }
  }

  void append(const Chunk_Ptr &ch, Time min, Time max) {
    logger("engine: io_adapter - add chunk #", ch->header->id, " id:",
           ch->header->first.id);
    const std::string sql_query =
        "INSERT INTO Chunks(number, meas_id,min_time,max_time,chunk_header, "
        "chunk_buffer) values (?,?,?,?,?,?);";
    sqlite3_stmt *pStmt;
    int rc;
    do {
      rc = sqlite3_prepare(_db, sql_query.c_str(), -1, &pStmt, 0);
      if (rc != SQLITE_OK) {
        auto err_msg = std::string(sqlite3_errmsg(_db));
        this->stop();
        THROW_EXCEPTION("engine: IOAdapter - ", err_msg);
      }

      sqlite3_bind_int64(pStmt, 1, ch->header->id);
      sqlite3_bind_int64(pStmt, 2, ch->header->first.id);
      sqlite3_bind_int64(pStmt, 3, min);
      sqlite3_bind_int64(pStmt, 4, max);
      sqlite3_bind_blob(pStmt, 5, ch->header, sizeof(ChunkHeader), SQLITE_STATIC);
      sqlite3_bind_blob(pStmt, 6, ch->_buffer_t, ch->header->size, SQLITE_STATIC);
      rc = sqlite3_step(pStmt);
      assert(rc != SQLITE_ROW);
      rc = sqlite3_finalize(pStmt);
    } while (rc == SQLITE_SCHEMA);
  }

  ChunksList readInterval(uint64_t period_from, uint64_t period_to, Id meas_id) {
    ChunksList result;
    const std::string sql_query = "SELECT chunk_header, chunk_buffer FROM Chunks WHERE "
                                  "meas_id=? AND number>=? AND number<=? ORDER BY number";
    sqlite3_stmt *pStmt;
    int rc;

    do {
      rc = sqlite3_prepare(_db, sql_query.c_str(), -1, &pStmt, 0);
      if (rc != SQLITE_OK) {
        auto err_msg = std::string(sqlite3_errmsg(_db));
        this->stop();
        THROW_EXCEPTION("engine: IOAdapter - ", err_msg);
      }
      sqlite3_bind_int64(pStmt, 1, meas_id);
      sqlite3_bind_int64(pStmt, 2, period_from);
      sqlite3_bind_int64(pStmt, 3, period_to);

      while (1) {
        rc = sqlite3_step(pStmt);
        if (rc == SQLITE_ROW) {
          using utils::inInterval;
          auto headerSize = sqlite3_column_bytes(pStmt, 0);
          assert(headerSize == sizeof(ChunkHeader));
          auto header_blob = (ChunkHeader *)sqlite3_column_blob(pStmt, 0);
          ChunkHeader *chdr = new ChunkHeader;
          memcpy(chdr, header_blob, headerSize);

          auto buffSize = sqlite3_column_bytes(pStmt, 1);
          assert(buffSize == chdr->size);
          uint8_t *buffer = new uint8_t[buffSize];
          memcpy(buffer, sqlite3_column_blob(pStmt, 1), buffSize);

          Chunk_Ptr cptr{new ZippedChunk(chdr, buffer)};
          cptr->is_owner = true;
          result.push_back(cptr);
        } else {
          break;
        }
      }
      rc = sqlite3_finalize(pStmt);
    } while (rc == SQLITE_SCHEMA);

    return result;
  }
  Chunk_Ptr readTimePoint(uint64_t period, Id meas_id) {
	Chunk_Ptr result;
    
    const std::string sql_query = "SELECT chunk_header, chunk_buffer FROM Chunks WHERE "
                                  "meas_id=? AND number==?";
    sqlite3_stmt *pStmt;
    int rc;

    do {
      rc = sqlite3_prepare(_db, sql_query.c_str(), -1, &pStmt, 0);
      if (rc != SQLITE_OK) {
        auto err_msg = std::string(sqlite3_errmsg(_db));
        this->stop();
        THROW_EXCEPTION("engine: IOAdapter - ", err_msg);
      }

      sqlite3_bind_int64(pStmt, 1, meas_id);
      sqlite3_bind_int64(pStmt, 2, period);

      while (1) {
        rc = sqlite3_step(pStmt);
        if (rc == SQLITE_ROW) {
          using utils::inInterval;
          auto headerSize = sqlite3_column_bytes(pStmt, 0);
          assert(headerSize == sizeof(ChunkHeader));
          auto header_blob = (ChunkHeader *)sqlite3_column_blob(pStmt, 0);

          ChunkHeader *chdr = new ChunkHeader;
          memcpy(chdr, header_blob, headerSize);

          auto buffSize = sqlite3_column_bytes(pStmt, 1);
          assert(buffSize == chdr->size);
          uint8_t *buffer = new uint8_t[buffSize];
          memcpy(buffer, sqlite3_column_blob(pStmt, 1), buffSize);

          Chunk_Ptr cptr{new ZippedChunk(chdr, buffer)};
          cptr->is_owner = true;
		  result = cptr;
        } else {
          break;
        }
      }
      rc = sqlite3_finalize(pStmt);
    } while (rc == SQLITE_SCHEMA);

    return result;
  }

  void replace(const Chunk_Ptr &ch, Time min, Time max) {
    logger("engine: io_adapter - replace chunk #", ch->header->id, " id:",
           ch->header->first.id);
    const std::string sql_query = "UPDATE Chunks SET min_time=?, max_time=?, "
                                  "chunk_header=?, chunk_buffer=? where number=? AND "
                                  "meas_id=?";
    sqlite3_stmt *pStmt;
    int rc;
    do {
      rc = sqlite3_prepare(_db, sql_query.c_str(), -1, &pStmt, 0);
      if (rc != SQLITE_OK) {
        auto err_msg = std::string(sqlite3_errmsg(_db));
        this->stop();
        THROW_EXCEPTION("engine: IOAdapter - ", err_msg);
      }

      sqlite3_bind_int64(pStmt, 1, min);
      sqlite3_bind_int64(pStmt, 2, max);
      sqlite3_bind_blob(pStmt, 3, ch->header, sizeof(ChunkHeader), SQLITE_STATIC);
      sqlite3_bind_blob(pStmt, 4, ch->_buffer_t, ch->header->size, SQLITE_STATIC);
      sqlite3_bind_int64(pStmt, 5, ch->header->id);
      sqlite3_bind_int64(pStmt, 6, ch->header->first.id);
      rc = sqlite3_step(pStmt);
      assert(rc != SQLITE_ROW);
      rc = sqlite3_finalize(pStmt);
    } while (rc == SQLITE_SCHEMA);
  }

  // min and max
  std::tuple<Time, Time> minMax() {
    logger("engine: io_adapter - minMax");
    const std::string sql_query = "SELECT min(min_time), max(max_time), count(max_time) FROM Chunks";
    sqlite3_stmt *pStmt;
    int rc;
    Time min = MAX_TIME;
    Time max = MIN_TIME;
    do {
      rc = sqlite3_prepare(_db, sql_query.c_str(), -1, &pStmt, 0);
      if (rc != SQLITE_OK) {
        auto err_msg = std::string(sqlite3_errmsg(_db));
        this->stop();
        THROW_EXCEPTION("engine: IOAdapter - ", err_msg);
      }

      rc = sqlite3_step(pStmt);
      if (rc == SQLITE_ROW) {
		  auto count = sqlite3_column_int64(pStmt, 2);
		  if (count > 0) {
			  min = sqlite3_column_int64(pStmt, 0);
			  max = sqlite3_column_int64(pStmt, 1);
		  }
      }

      rc = sqlite3_finalize(pStmt);
    } while (rc == SQLITE_SCHEMA);

    return std::tie(min, max);
  }

  Time minTime() { return std::get<0>(minMax()); }

  Time maxTime() { return std::get<1>(minMax()); }

  bool minMaxTime(Id id, Time *minResult, Time *maxResult) {
	  logger("engine: io_adapter - minMaxTime #",id);
	  const std::string sql_query = "SELECT min(min_time), max(max_time), count(max_time) FROM Chunks WHERE meas_id=?";
	  sqlite3_stmt *pStmt;
	  int rc;
	  *minResult = MAX_TIME;
	  *maxResult = MIN_TIME;
	  bool result = false;
	  do {
		  rc = sqlite3_prepare(_db, sql_query.c_str(), -1, &pStmt, 0);
		  if (rc != SQLITE_OK) {
			  auto err_msg = std::string(sqlite3_errmsg(_db));
			  this->stop();
			  THROW_EXCEPTION("engine: IOAdapter - ", err_msg);
		  }
		  sqlite3_bind_int64(pStmt, 1, id);
		  rc = sqlite3_step(pStmt);
		  if (rc == SQLITE_ROW) {
			  auto count = sqlite3_column_int64(pStmt, 2);
			  if (count > 0) {
				  *minResult = sqlite3_column_int64(pStmt, 0);
				  *maxResult = sqlite3_column_int64(pStmt, 1);
				  result = true;
			  }
		  }

		  rc = sqlite3_finalize(pStmt);
	  } while (rc == SQLITE_SCHEMA);

	  return result;
  }

  void init_tables() {
    logger_info("engine: io_adapter - init_tables");
    char *err = 0;
    if (sqlite3_exec(_db, BYSTEP_CREATE_SQL, 0, 0, &err)) {
      auto err_msg = sqlite3_errmsg(_db);
      sqlite3_free(err);
      this->stop();
      THROW_EXCEPTION("Can't init database: ", err_msg);
    }
  }

  sqlite3 *_db;
};

IOAdapter::IOAdapter(const std::string &fname) : _impl(new IOAdapter::Private(fname)) {}

IOAdapter::~IOAdapter() {
  _impl = nullptr;
}

void IOAdapter::stop() {
  _impl->stop();
}

void IOAdapter::append(const Chunk_Ptr &ch, Time min, Time max) {
  _impl->append(ch,min,max);
}

ChunksList IOAdapter::readInterval(uint64_t period_from, uint64_t period_to, Id meas_id) {
  return _impl->readInterval(period_from,period_to,meas_id);
}

Chunk_Ptr IOAdapter::readTimePoint(uint64_t period, Id meas_id) {
  return _impl->readTimePoint(period, meas_id);
}

void IOAdapter::replace(const Chunk_Ptr &ch, Time min, Time max) {
  return _impl->replace(ch, min,max);
}

Time IOAdapter::minTime() {
  return _impl->minTime();
}

Time IOAdapter::maxTime() {
  return _impl->maxTime();
}

bool IOAdapter::minMaxTime(Id id, Time *minResult, Time *maxResult) {
  return _impl->minMaxTime(id, minResult, maxResult);
}