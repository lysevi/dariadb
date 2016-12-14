#include <libdariadb/storage/bystep/io_adapter.h>
#include <libdariadb/utils/logger.h>
#include <cassert>
#include <extern/libsqlite3/sqlite3.h>
#include <string>

using namespace dariadb;
using namespace dariadb::storage;

const char *BYSTEP_CREATE_SQL =
    "CREATE TABLE IF NOT EXISTS Chunks("
	"id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "number INTEGER," //chunk::id
    "meas_id INTEGER," // measurement id
    "min_time INTEGER,"
    "max_time INTEGER,"
    "chunk_header blob,"
    "chunk_buffer blob);";

class IOAdapter::Private {
public:
  Private(const std::string &fname) {
    _db = nullptr;
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
    }
	logger("engine: io_adapter - stoped");
  }

  void append(const Chunk_Ptr &ch) {
	  logger("engine: io_adapter - add chunk #", ch->header->id, " id:", ch->header->first.id);
    const std::string sql_query = "INSERT INTO Chunks(number, meas_id,min_time,max_time,chunk_header, "
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
      sqlite3_bind_int64(pStmt, 3, ch->header->minTime);
      sqlite3_bind_int64(pStmt, 4, ch->header->maxTime);
      sqlite3_bind_blob(pStmt, 5, ch->header, sizeof(ChunkHeader), SQLITE_STATIC);
      sqlite3_bind_blob(pStmt, 6, ch->_buffer_t, ch->header->size, SQLITE_STATIC);
      rc = sqlite3_step(pStmt);
      assert(rc != SQLITE_ROW);
      rc = sqlite3_finalize(pStmt);
    } while (rc == SQLITE_SCHEMA);
  }

  ChunksList readInterval(const QueryInterval &query) {
    ChunksList result;
    // TODO generate string with needed period and id as variable. (select chunk_header...
    // WHERE id=? and from<'..' and to>'...'
    const std::string sql_query =
        "SELECT chunk_header, chunk_buffer FROM Chunks WHERE meas_id=? ORDER BY number";
    sqlite3_stmt *pStmt;
    int rc;
    for (auto id : query.ids) {
      do {
        rc = sqlite3_prepare(_db, sql_query.c_str(), -1, &pStmt, 0);
        if (rc != SQLITE_OK) {
          auto err_msg = std::string(sqlite3_errmsg(_db));
          this->stop();
          THROW_EXCEPTION("engine: IOAdapter - ", err_msg);
        }
        sqlite3_bind_int64(pStmt, 1, id);

        while (1) {
          rc = sqlite3_step(pStmt);
          if (rc == SQLITE_ROW) {
            using utils::inInterval;
            auto headerSize = sqlite3_column_bytes(pStmt, 0);
            assert(headerSize == sizeof(ChunkHeader));
            auto header_blob = (ChunkHeader *)sqlite3_column_blob(pStmt, 0);
            if (inInterval(query.from, query.to, header_blob->minTime) ||
                inInterval(query.from, query.to, header_blob->maxTime) ||
                inInterval(header_blob->minTime, header_blob->maxTime, query.from) ||
                inInterval(header_blob->minTime, header_blob->maxTime, query.to)) {
              ChunkHeader *chdr = new ChunkHeader;
              memcpy(chdr, header_blob, headerSize);

              auto buffSize = sqlite3_column_bytes(pStmt, 1);
              assert(buffSize == chdr->size);
              uint8_t *buffer = new uint8_t[buffSize];
              memcpy(buffer, sqlite3_column_blob(pStmt, 1), buffSize);

              Chunk_Ptr cptr{new ZippedChunk(chdr, buffer)};
              cptr->is_owner = true;
              result.push_back(cptr);
            }
          } else {
            break;
          }
        }
        rc = sqlite3_finalize(pStmt);
      } while (rc == SQLITE_SCHEMA);
    }

    return result;
  }
  IdToChunkMap readTimePoint(const QueryTimePoint &query) {
    IdToChunkMap result;
    // TODO generate string with needed period and id as variable
    const std::string sql_query = "SELECT chunk_header, chunk_buffer FROM Chunks WHERE "
                                  "meas_id=? AND min_time<=?";
    sqlite3_stmt *pStmt;
    int rc;

    for (auto id : query.ids) {
      do {
        rc = sqlite3_prepare(_db, sql_query.c_str(), -1, &pStmt, 0);
        if (rc != SQLITE_OK) {
          auto err_msg = std::string(sqlite3_errmsg(_db));
          this->stop();
          THROW_EXCEPTION("engine: IOAdapter - ", err_msg);
        }

        sqlite3_bind_int64(pStmt, 1, id);
        sqlite3_bind_int64(pStmt, 2, query.time_point);

        while (1) {
          rc = sqlite3_step(pStmt);
          if (rc == SQLITE_ROW) {
            using utils::inInterval;
            auto headerSize = sqlite3_column_bytes(pStmt, 0);
            assert(headerSize == sizeof(ChunkHeader));
            auto header_blob = (ChunkHeader *)sqlite3_column_blob(pStmt, 0);
            auto fres = result.find(id);
            if (fres == result.end() ||
                fres->second->header->maxTime < header_blob->maxTime) {
              ChunkHeader *chdr = new ChunkHeader;
              memcpy(chdr, header_blob, headerSize);

              auto buffSize = sqlite3_column_bytes(pStmt, 1);
              assert(buffSize == chdr->size);
              uint8_t *buffer = new uint8_t[buffSize];
              memcpy(buffer, sqlite3_column_blob(pStmt, 1), buffSize);

              Chunk_Ptr cptr{new ZippedChunk(chdr, buffer)};
              cptr->is_owner = true;
              result[id] = cptr;
            }
          } else {
            break;
          }
        }
        rc = sqlite3_finalize(pStmt);
      } while (rc == SQLITE_SCHEMA);
    }

    return result;
  }

  void replace(const Chunk_Ptr &ch) {
	  logger("engine: io_adapter - replace chunk #", ch->header->id, " id:", ch->header->first.id);
	  const std::string sql_query = "UPDATE Chunks SET min_time=?, max_time=?, chunk_header=?, chunk_buffer=? where number=? AND meas_id=?";
	  sqlite3_stmt *pStmt;
	  int rc;
	  do {
		  rc = sqlite3_prepare(_db, sql_query.c_str(), -1, &pStmt, 0);
		  if (rc != SQLITE_OK) {
			  auto err_msg = std::string(sqlite3_errmsg(_db));
			  this->stop();
			  THROW_EXCEPTION("engine: IOAdapter - ", err_msg);
		  }

		  sqlite3_bind_int64(pStmt, 1, ch->header->minTime);
		  sqlite3_bind_int64(pStmt, 2, ch->header->maxTime);
		  sqlite3_bind_blob(pStmt, 3, ch->header, sizeof(ChunkHeader), SQLITE_STATIC);
		  sqlite3_bind_blob(pStmt, 4, ch->_buffer_t, ch->header->size, SQLITE_STATIC);
		  sqlite3_bind_int64(pStmt, 5, ch->header->id);
		  sqlite3_bind_int64(pStmt, 6, ch->header->first.id);
		  rc = sqlite3_step(pStmt);
		  assert(rc != SQLITE_ROW);
		  rc = sqlite3_finalize(pStmt);
	  } while (rc == SQLITE_SCHEMA);
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

void IOAdapter::append(const Chunk_Ptr &ch) {
  _impl->append(ch);
}

ChunksList IOAdapter::readInterval(const QueryInterval &query) {
  return _impl->readInterval(query);
}

IdToChunkMap IOAdapter::readTimePoint(const QueryTimePoint &query) {
  return _impl->readTimePoint(query);
}

void IOAdapter::replace(const Chunk_Ptr &ch) {
  return _impl->replace(ch);
}