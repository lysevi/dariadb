#include <libdariadb/storage/bystep/bystep_storage.h>
#include <libdariadb/storage/settings.h>
#include <libdariadb/utils/fs.h>
#include <libsqlite3/sqlite3.h>

using namespace dariadb;
using namespace dariadb::storage;

const char *BYSTEP_CREATE_SQL = "CREATE TABLE IF NOT EXISTS values(id INTEGER PRIMARY KEY "
                         "AUTOINCREMENT, chunk blob); ";
;
const char *filename = "bystep.db";

struct ByStepStorage::Private : public IMeasStorage {
  Private(const EngineEnvironment_ptr &env)
      : _env(env), _settings(_env->getResourceObject<Settings>(
                       EngineEnvironment::Resource::SETTINGS)) {
    stoped = false;

    auto fname = utils::fs::append_path(_settings->path, filename);
    logger_info("engine: opening  bystep storage...");
    int rc = sqlite3_open(fname.c_str(), &db);
    if (rc) {
      auto err_msg = sqlite3_errmsg(db);
      THROW_EXCEPTION("Can't open database: ", err_msg);
    }

    logger_info("engine: bystep storage file opened.");
    char *err = 0;
    if (sqlite3_exec(db, BYSTEP_CREATE_SQL, 0, 0, &err)) {
      fprintf(stderr, "Îøèáêà SQL: %sn", err);
      sqlite3_free(err);
    }
  }
  void stop() {
    if (!stoped) {
      sqlite3_close(db);
      stoped = true;
    }
  }
  ~Private() { stop(); }

  Status append(const Meas &value) override {
    NOT_IMPLEMENTED;
    return Status(1, 0);
  }

  Id2MinMax loadMinMax() override {
    Id2MinMax result;
    NOT_IMPLEMENTED;
    return result;
  }

  Time minTime() override {
    Time result = MAX_TIME;
    NOT_IMPLEMENTED;
    return result;
  }
  virtual Time maxTime() override {
    Time result = MIN_TIME;
    NOT_IMPLEMENTED;
    return result;
  }

  virtual bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                          dariadb::Time *maxResult) override {
    NOT_IMPLEMENTED;
    return false;
  }

  void foreach (const QueryInterval &q, IReaderClb * clbk) override { NOT_IMPLEMENTED; }

  virtual Id2Meas readTimePoint(const QueryTimePoint &q) override {
    NOT_IMPLEMENTED;
    return Id2Meas();
  }

  virtual Id2Meas currentValue(const IdArray &ids, const Flag &flag) override {
    NOT_IMPLEMENTED;
    return Id2Meas();
  }

  void flush() override {}

  EngineEnvironment_ptr _env;
  storage::Settings *_settings;
  sqlite3 *db;
  bool stoped;
};

ByStepStorage::ByStepStorage(const EngineEnvironment_ptr &env)
    : _impl(new ByStepStorage::Private(env)) {}

ByStepStorage::~ByStepStorage() {
  _impl = nullptr;
}

Time ByStepStorage::minTime() {
  return _impl->minTime();
}

Time ByStepStorage::maxTime() {
  return _impl->maxTime();
}

bool ByStepStorage::minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                               dariadb::Time *maxResult) {
  return _impl->minMaxTime(id, minResult, maxResult);
}

void ByStepStorage::foreach (const QueryInterval &q, IReaderClb * clbk) {
  _impl->foreach (q, clbk);
}

Id2Meas ByStepStorage::readTimePoint(const QueryTimePoint &q) {
  return _impl->readTimePoint(q);
}

Id2Meas ByStepStorage::currentValue(const IdArray &ids, const Flag &flag) {
  return _impl->currentValue(ids, flag);
}

Status ByStepStorage::append(const Meas &value) {
  return _impl->append(value);
}

void ByStepStorage::flush() {
  _impl->flush();
}

void ByStepStorage::stop() {
  _impl->stop();
}

Id2MinMax ByStepStorage::loadMinMax() {
  return _impl->loadMinMax();
}
