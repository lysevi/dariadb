#pragma once

#include "storage.h"
#include "storage/aof_manager.h"
#include "storage/capacitor_manager.h"
#include "storage/page_manager.h"
#include "utils/utils.h"
#include <memory>

namespace dariadb {
namespace storage {

class Engine : public MeasStorage {
public:
  struct QueueSizes {
    size_t aofs_count;   ///  AOF count
    size_t pages_count;  /// pages count
    size_t cola_count;   /// COLA files count.
    size_t active_works; /// async tasks runned.
  };
  struct GCResult {
    PageManager::GCResult page_result;
  };
  struct Limits {
    size_t max_mem_chunks; // max_mem_chunks - maximum chunks in memory.zero
                           // - by old_mem_chunks(MemStorage)

    Limits(const size_t max_mem) { max_mem_chunks = max_mem; }
  };

  Engine() = delete;
  Engine(const Engine &) = delete;
  Engine &operator=(const Engine &) = delete;
  virtual ~Engine();

  ///
  /// \brief Engine
  /// \param page_manager_params - params of page manager (PageManager)
  /// \param cap_params - capacitor params  (Capacitor)
  Engine(storage::AOFManager::Params aof_params,
         storage::PageManager::Params page_manager_params,
         dariadb::storage::CapacitorManager::Params cap_params, const Limits &limits);

  append_result append(const Meas &value) override;

  void flush() override;

  QueueSizes queue_size() const;

  virtual void foreach (const QueryInterval &q, ReaderClb * clbk);
  virtual Meas::MeasList readInterval(const QueryInterval &q) override;
  virtual Meas::Id2Meas readInTimePoint(const QueryTimePoint &q) override;
  virtual Meas::Id2Meas currentValue(const IdArray &ids, const Flag &flag) override;

  Time minTime() override;
  Time maxTime() override;
  bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                  dariadb::Time *maxResult) override;

  Id load(const QueryInterval &qi);
  Id load(const QueryTimePoint &qt);
  Meas::MeasList getResult(Id);

  void drop_part_caps(size_t count);
  GCResult gc();
  void subscribe(const IdArray &ids, const Flag &flag, const ReaderClb_ptr &clbk);
  void wait_all_asyncs();

protected:
  class Private;
  std::unique_ptr<Private> _impl;
};
}
}
