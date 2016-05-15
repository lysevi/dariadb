#pragma once

#include "storage.h"
#include "storage/capacitor.h"
#include "storage/chunk_container.h"
#include "storage/mode.h"
#include "storage/page_manager.h"
#include "utils/utils.h"
#include <memory>

namespace dariadb {
namespace storage {

class Engine : public MeasStorage {
public:
  struct QueueSizes {
    size_t page; /// queue length in PageManager
    size_t cap;  /// measurements in Capacitor
  };

  struct Limits {
    dariadb::Time old_mem_chunks; // old_mem_chunks - time when drop old
                                  // chunks to page (MemStorage)
    size_t max_mem_chunks; // max_mem_chunks - maximum chunks in memory.zero
                           // - by old_mem_chunks(MemStorage)

    Limits(const dariadb::Time old_time, const size_t max_mem) {
      old_mem_chunks = old_time;
      max_mem_chunks = max_mem;
    }
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
  //  virtual Reader_ptr readInterval(Time from, Time to) override;
  //  virtual Reader_ptr readInTimePoint(Time time_point) override;
  virtual Reader_ptr readInterval(const QueryInterval &q) override;
  virtual Reader_ptr readInTimePoint(const QueryTimePoint &q) override;

  Time minTime() override;
  Time maxTime() override;

protected:
  class Private;
  std::unique_ptr<Private> _impl;
};
}
}
