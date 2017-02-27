#pragma once
#include <libdariadb/interfaces/imeasstorage.h>
#include <libdariadb/storage/dropper_description.h>
#include <libdariadb/storage/memstorage/description.h>
#include <libdariadb/storage/settings.h>
#include <memory>
namespace dariadb {

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
  virtual void eraseOld(const Time &t) = 0;
  virtual void repack() = 0;
  virtual void stop() = 0;
  virtual void wait_all_asyncs() = 0;
  virtual void drop_part_wals(size_t count) = 0;
  virtual storage::Settings_ptr settings()=0;
};
using IEngine_Ptr = std::shared_ptr<IEngine>;
}
