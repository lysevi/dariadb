#include <libdariadb/storage/bystep/io_adapter.h>
#include <libdariadb/flags.h>
#include <libdariadb/utils/logger.h>
#include <libdariadb/utils/async/thread_manager.h>
#include <cassert>
#include <extern/libsqlite3/sqlite3.h>
#include <string>
#include <list>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <ctime>
#include <atomic>
#include <cstring>

using namespace dariadb;
using namespace dariadb::storage;
using namespace dariadb::utils;
using namespace dariadb::utils::async;

const char *BYSTEP_CREATE_SQL = "PRAGMA page_size = 4096;"  
                                "PRAGMA journal_mode =WAL;"
                                "CREATE TABLE IF NOT EXISTS Chunks("
                                "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                                "number INTEGER,"  // chunk::id and period number.
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
	_under_drop = 0;
    init_tables();
	_stop_flag = false;
	_write_thread = std::thread(std::bind(&IOAdapter::Private::write_thread_func, this));
  }
  ~Private() { stop(); }

  void stop() {
    if (_db != nullptr) {
		flush();
		_stop_flag = true;
		_cond_var.notify_all();
		_write_thread.join();
      sqlite3_close(_db);
      _db = nullptr;
      logger("engine: io_adapter - stoped");
    }
  }
  struct ChunkMinMax {
	  Chunk_Ptr ch;
	  Time min;
	  Time max;
  };

  void append(const Chunk_Ptr &ch, Time min, Time max) {
	  ChunkMinMax cmm;
	  cmm.ch = ch;
	  cmm.min = min;
	  cmm.max = max;
	  std::lock_guard<std::mutex> lg(_chunks_list_locker);
	  _chunks_list.push_back(cmm);
	  ++_under_drop;
	  _cond_var.notify_all();
  }

  void _append(const Chunk_Ptr &ch, Time min, Time max) {
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

	  //TODO move to method.
	  auto cur_chunk_buf_size = ch->header->size - ch->header->bw_pos + 1;
	  auto skip_count = ch->header->size - cur_chunk_buf_size;
	  ch->header->size = cur_chunk_buf_size;
		
      sqlite3_bind_int64(pStmt, 1, ch->header->id);
      sqlite3_bind_int64(pStmt, 2, ch->header->first.id);
      sqlite3_bind_int64(pStmt, 3, min);
      sqlite3_bind_int64(pStmt, 4, max);
      sqlite3_bind_blob(pStmt, 5, ch->header, sizeof(ChunkHeader), SQLITE_STATIC);
      sqlite3_bind_blob(pStmt, 6, ch->_buffer_t+skip_count, ch->header->size, SQLITE_STATIC);
      rc = sqlite3_step(pStmt);
      assert(rc != SQLITE_ROW);
      rc = sqlite3_finalize(pStmt);
    } while (rc == SQLITE_SCHEMA);
  }

  ChunksList readInterval(uint64_t period_from, uint64_t period_to, Id meas_id) {
	  lock();
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

	IdSet ids;
	//TODO move to method
	for (auto&cmm : _chunks_list) {
		ids.insert(cmm.ch->header->first.id);
		if (cmm.ch->header->first.id == meas_id){
			if (utils::inInterval(period_from, period_to, cmm.ch->header->id)) {
				result.push_back(cmm.ch);
			}
		}
	}
	unlock();
	std::vector<Chunk_Ptr> ch_vec{ result.begin(), result.end() };
	std::sort(ch_vec.begin(), ch_vec.end(), [](Chunk_Ptr ch1, Chunk_Ptr ch2) {return ch1->header->id < ch2->header->id; });
	
	return ChunksList(ch_vec.begin(), ch_vec.end());
  }

  Chunk_Ptr readTimePoint(uint64_t period, Id meas_id) {
	lock();
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

	if (result == nullptr) {
		for (auto&cmm : _chunks_list) {
			if (cmm.ch->header->id == period&&cmm.ch->header->first.id == meas_id) {
				result = cmm.ch;
				break;
			}
		}
	}
	unlock();
    return result;
  }

  Id2Meas currentValue() {
    lock();
    Id2Meas result;
    const std::string sql_query = "SELECT a.chunk_header, a.chunk_buffer FROM "
                                  "Chunks a INNER JOIN(select id, number, MAX(number) "
                                  "from Chunks GROUP BY id) b ON a.id = b.id AND "
                                  "a.number = b.number";
     sqlite3_stmt *pStmt;
     int rc;

     do {
       rc = sqlite3_prepare(_db, sql_query.c_str(), -1, &pStmt, 0);
       if (rc != SQLITE_OK) {
         auto err_msg = std::string(sqlite3_errmsg(_db));
         this->stop();
         THROW_EXCEPTION("engine: IOAdapter - ", err_msg);
       }

       while (1) {
         rc = sqlite3_step(pStmt);
         if (rc == SQLITE_ROW) {
           using utils::inInterval;
		   ChunkHeader *chdr = (ChunkHeader *)sqlite3_column_blob(pStmt, 0);
#ifdef DEBUG
		   auto headerSize = sqlite3_column_bytes(pStmt, 1);
		   assert(headerSize == sizeof(ChunkHeader));
#endif // DEBUG

		   uint8_t *buffer = (uint8_t *)sqlite3_column_blob(pStmt, 1);
		   //TODO move to method
           Chunk_Ptr cptr{new ZippedChunk(chdr, buffer)};
		   auto reader=cptr->getReader();
		   while (!reader->is_end()) {
			   auto v=reader->readNext();
			   if (v.flag != Flags::_NO_DATA) {
				   if (result[v.id].time < v.time) {
					   result[v.id] = v;
				   }
			   }
		   }
         } else {
           break;
         }
       }
       rc = sqlite3_finalize(pStmt);
     } while (rc == SQLITE_SCHEMA);
	 
	 //TODO move to method
	 for (auto&cmm : _chunks_list) {
		 auto reader = cmm.ch->getReader();
		 while (!reader->is_end()) {
			 auto v = reader->readNext();
			 if (v.flag != Flags::_NO_DATA) {
				 if (result[v.id].time < v.time) {
					 result[v.id] = v;
				 }
			 }
		 }
	 }
     unlock();
    return result;
  }
  void replace(const Chunk_Ptr &ch, Time min, Time max) {
	  this->flush();
	  lock();
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
	unlock();
  }

  // min and max
  std::tuple<Time, Time> minMax() {
	lock();

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

	for (auto&cmm : _chunks_list) {
		min = std::min(min, cmm.min);
		max = std::min(max, cmm.max);
	}
	unlock();
    return std::tie(min, max);
  }

  Time minTime() { return std::get<0>(minMax()); }

  Time maxTime() { return std::get<1>(minMax()); }

  bool minMaxTime(Id id, Time *minResult, Time *maxResult) {

	  lock();
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

	  for (auto&cmm : _chunks_list) {
		  if (cmm.ch->header->first.id == id) {
			  result = true;
			  *minResult = std::min(*minResult, cmm.min);
			  *maxResult = std::min(*maxResult, cmm.max);
		  }
	  }
	  unlock();
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

  void lock() {
	  std::lock(_dropper_locker, _chunks_list_locker);
  }

  void unlock() {
	  _dropper_locker.unlock();
	  _chunks_list_locker.unlock();
  }

  void write_thread_func() {
    while (!_stop_flag) {
		std::unique_lock<std::mutex> lock(_chunks_list_locker);
		_cond_var.wait(lock);

      if (_stop_flag && _chunks_list.empty()) {
        break;
      }
      while (!_dropper_locker.try_lock()) {
		  std::this_thread::yield();
      }

      std::list<ChunkMinMax> local_copy{_chunks_list.begin(), _chunks_list.end()};
      assert(local_copy.size() == _chunks_list.size());
      _under_drop = local_copy.size();
      _chunks_list.clear();
	  lock.unlock();

      auto start_time = clock();
	  //TODO write in DISK_IO pool.
      sqlite3_exec(_db, "BEGIN TRANSACTION;", NULL, NULL, NULL);

      for (auto &c : local_copy) {
        this->_append(c.ch, c.min, c.max);
      }
      sqlite3_exec(_db, "END TRANSACTION;", NULL, NULL, NULL);

      auto end = clock();
      auto elapsed = double(end - start_time) / CLOCKS_PER_SEC;

      logger("engine: io_adapter - write ", local_copy.size(), " chunks. elapsed time - ",
             elapsed);
	  _under_drop -= local_copy.size();
      _dropper_locker.unlock();
    }
    logger_info("engine: io_adapter - stoped.");
  }

  void flush() {
	  logger_info("engine: io_adapter - flush.");
	  while (_under_drop!=0) {//TODO make more smarter.
		  std::this_thread::sleep_for(std::chrono::milliseconds(100));
		  _cond_var.notify_all();
	  }
  }

  bystep::Description description() {
    bystep::Description result;
    _chunks_list_locker.lock();
    result.in_queue = _under_drop;
    _chunks_list_locker.unlock();
    return result;
  }

  void eraseOld(uint64_t period_from, uint64_t period_to, Id id) {
	  lock();
	  logger("engine: io_adapter - erase id:", id);
	  const std::string sql_query = " DELETE FROM Chunks WHERE meas_id=? AND number>=? AND number<?;";
	  sqlite3_stmt *pStmt;
	  int rc;
	  do {
		  rc = sqlite3_prepare(_db, sql_query.c_str(), -1, &pStmt, 0);
		  if (rc != SQLITE_OK) {
			  auto err_msg = std::string(sqlite3_errmsg(_db));
			  this->stop();
			  THROW_EXCEPTION("engine: IOAdapter - ", err_msg);
		  }

		  sqlite3_bind_int64(pStmt, 1, id);
		  sqlite3_bind_int64(pStmt, 2, period_from);
		  sqlite3_bind_int64(pStmt, 3, period_to);

		  rc = sqlite3_step(pStmt);
		  assert(rc != SQLITE_ROW);
		  rc = sqlite3_finalize(pStmt);
	  } while (rc == SQLITE_SCHEMA);
	  unlock();
  }

  sqlite3 *_db;
  bool                  _stop_flag;
  std::list<ChunkMinMax> _chunks_list;
  std::mutex            _chunks_list_locker;
  std::condition_variable _cond_var;
  std::mutex            _dropper_locker;
  std::atomic_int       _under_drop;
  std::thread           _write_thread;
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

Id2Meas IOAdapter::currentValue() {
	return _impl->currentValue();
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

void IOAdapter::flush() {
	_impl->flush();
}

bystep::Description IOAdapter::description() {
	return _impl->description();
}

void IOAdapter::eraseOld(uint64_t period_from, uint64_t period_to, Id id) {
	_impl->eraseOld(period_from, period_to, id);
}
