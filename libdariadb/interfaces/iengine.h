#pragma once
#include <libdariadb/interfaces/icompactioncontroller.h>
#include <libdariadb/interfaces/imeasstorage.h>
#include <libdariadb/scheme/ischeme.h>
#include <libdariadb/storage/dropper_description.h>
#include <libdariadb/storage/memstorage/description.h>
#include <libdariadb/storage/settings.h>
#include <memory>
namespace dariadb {

struct foreach_async_data {
  size_t next_id_pos;
  QueryInterval q;
  IReadCallback *clbk;

  foreach_async_data(const QueryInterval &oq, IReadCallback *oclbk) : q(oq), clbk(oclbk) {
    next_id_pos = size_t(0);
  }
};

class IEngine : public IMeasStorage {
public:
  struct Description {
    size_t wal_count;    ///  wal count.
    size_t pages_count;  /// pages count.
    size_t active_works; /// async tasks runned.
    storage::DropperDescription dropper;
    storage::memstorage::Description memstorage;

    Description() { wal_count = pages_count = active_works = size_t(0); }

    void update(const Description &other) {
      wal_count += other.wal_count;
      pages_count += other.pages_count;
      dropper.wal += other.dropper.wal;
      memstorage.allocated += other.memstorage.allocated;
      memstorage.allocator_capacity = other.memstorage.allocator_capacity;
    }
  };
  virtual Description description() const = 0;
  virtual void fsck() = 0;
  virtual void eraseOld(const Id id, const Time t) = 0;
  virtual void repack(dariadb::Id id) = 0;
  virtual void compact(ICompactionController *logic) = 0;
  virtual void stop() = 0;
  virtual void wait_all_asyncs() = 0;
  virtual void compress_all() = 0;
  virtual storage::Settings_ptr settings() = 0;
  virtual STRATEGY strategy() const = 0;
  virtual void subscribe(const IdArray &ids, const Flag &flag,
                         const ReaderCallback_ptr &clbk) = 0;
  EXPORT void foreach (const QueryInterval &q, IReadCallback * clbk) override final;
  EXPORT void foreach (const QueryTimePoint &q, IReadCallback * clbk) override final;

  EXPORT void setScheme(const scheme::IScheme_Ptr &scheme) { _scheme = scheme; }
  EXPORT scheme::IScheme_Ptr getScheme() { return _scheme; }

  scheme::IScheme_Ptr _scheme;
};

using IEngine_Ptr = std::shared_ptr<IEngine>;
} // namespace dariadb
