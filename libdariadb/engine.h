#pragma once

#include "storage.h"
#include "storage/capacitor.h"
#include "storage/chunk_container.h"
#include "storage/page_manager.h"
#include "utils/utils.h"
#include <memory>

namespace dariadb {
namespace storage {

class Engine : public MeasStorage {
public:
  struct QueueSizes {
    size_t pages_count; /// pages count
    size_t cola_count;  /// COLA files count.
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
  Engine(storage::PageManager::Params page_manager_params,
         dariadb::storage::Capacitor::Params cap_params, const Limits &limits);

  append_result append(const Meas &value) override;
  void subscribe(const IdArray &ids, const Flag &flag,
                 const ReaderClb_ptr &clbk) override;
  Reader_ptr currentValue(const IdArray &ids, const Flag &flag) override;
  void flush() override;

  QueueSizes queue_size() const;

  // Inherited via MeasStorage
  virtual Reader_ptr readInterval(const QueryInterval &q) override;
  virtual Reader_ptr readInTimePoint(const QueryTimePoint &q) override;

  Time minTime() override;
  Time maxTime() override;
  bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                  dariadb::Time *maxResult) override;

  Id load(const QueryInterval &qi);
  Id load(const QueryTimePoint &qt);
  Meas::MeasList getResult(Id);

protected:
  class Private;
  std::unique_ptr<Private> _impl;
};
}
}
