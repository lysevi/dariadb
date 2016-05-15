#include "engine.h"
#include "storage/capacitor.h"
#include "storage/memstorage.h"
#include "storage/page_manager.h"
#include "storage/subscribe.h"
#include "utils/exception.h"
#include "utils/locker.h"
#include <algorithm>
#include <cassert>

using namespace dariadb;
using namespace dariadb::storage;

class Engine::Private {
public:
  Private(const PageManager::Params &page_storage_params,
          dariadb::storage::Capacitor::Params cap_params,
          dariadb::storage::Engine::Limits limits)
      : mem_storage{new MemoryStorage()},
        _page_manager_params(page_storage_params), _cap_params(cap_params),
        _limits(limits) {
    _subscribe_notify.start();
    mem_cap = new Capacitor(PageManager::instance(), _cap_params);
    mem_storage_raw = dynamic_cast<MemoryStorage *>(mem_storage.get());
    assert(mem_storage_raw != nullptr);

    PageManager::start(_page_manager_params);

    // auto open_chunks = PageManager::instance()->get_open_chunks();
    // mem_storage_raw->append(open_chunks);
    // mem_storage_raw->set_chunkWriter(PageManager::instance());
  }
  ~Private() {
    _subscribe_notify.stop();
    this->flush();
    // if (_limits.max_mem_chunks != 0) {
    //   auto all_chunks = this->mem_storage_raw->drop_all();
    //   //PM
    ////PageManager::instance()->append(all_chunks); // use specified in ctor
    // }
    delete mem_cap;
    PageManager::stop();
  }

  Time minTime() {
    std::lock_guard<std::recursive_mutex> lg(_locker);
    if (PageManager::instance()->chunks_in_cur_page() > 0) {
      return PageManager::instance()->minTime();
    } else {
      return mem_storage->minTime();
    }
  }

  Time maxTime() { return mem_storage->maxTime(); }

  append_result append(const Meas &value) {
    append_result result{};
    if (mem_cap->append(value).writed != 1) {
      // if(mem_storage_raw->append(value).writed!=1){
      assert(false);
      result.ignored++;
    } else {
      _subscribe_notify.on_append(value);
      result.writed++;
    }

    return result;
  }

  void subscribe(const IdArray &ids, const Flag &flag,
                 const ReaderClb_ptr &clbk) {
    auto new_s = std::make_shared<SubscribeInfo>(ids, flag, clbk);
    _subscribe_notify.add(new_s);
  }

  Reader_ptr currentValue(const IdArray &ids, const Flag &flag) {
    return mem_storage->currentValue(ids, flag);
  }

  void flush() {
    std::lock_guard<std::recursive_mutex> lg(_locker);
    this->mem_cap->flush();
    PageManager::instance()->flush();
  }

  Engine::QueueSizes queue_size() const {
    QueueSizes result;
    result.page = PageManager::instance()->in_queue_size();
    result.cap = this->mem_cap->in_queue_size();
    return result;
  }

  // Inherited via MeasStorage
  Reader_ptr readInterval(Time from, Time to) { NOT_IMPLEMENTED }

  Reader_ptr readInTimePoint(Time time_point) { NOT_IMPLEMENTED }

  Reader_ptr readInterval(const QueryInterval &q) { NOT_IMPLEMENTED }

  Reader_ptr readInTimePoint(const QueryTimePoint &q) { NOT_IMPLEMENTED }

protected:
  std::shared_ptr<MemoryStorage> mem_storage;
  storage::MemoryStorage *mem_storage_raw;
  storage::Capacitor *mem_cap;

  storage::PageManager::Params _page_manager_params;
  dariadb::storage::Capacitor::Params _cap_params;
  dariadb::storage::Engine::Limits _limits;

  mutable std::recursive_mutex _locker;
  SubscribeNotificator _subscribe_notify;
};

Engine::Engine(storage::PageManager::Params page_manager_params,
               dariadb::storage::Capacitor::Params cap_params,
               const dariadb::storage::Engine::Limits &limits)
    : _impl{new Engine::Private(page_manager_params, cap_params, limits)} {}

Engine::~Engine() {
  _impl = nullptr;
}

Time Engine::minTime() {
  return _impl->minTime();
}

Time Engine::maxTime() {
  return _impl->maxTime();
}

append_result Engine::append(const Meas &value) {
  return _impl->append(value);
}

void Engine::subscribe(const IdArray &ids, const Flag &flag,
                       const ReaderClb_ptr &clbk) {
  _impl->subscribe(ids, flag, clbk);
}

Reader_ptr Engine::currentValue(const IdArray &ids, const Flag &flag) {
  return _impl->currentValue(ids, flag);
}

void Engine::flush() {
  _impl->flush();
}

Engine::QueueSizes Engine::queue_size() const {
  return _impl->queue_size();
}

// Reader_ptr dariadb::storage::Engine::readInterval(Time from, Time to){
//	return _impl->readInterval(from, to);
//}

// Reader_ptr dariadb::storage::Engine::readInTimePoint(Time time_point) {
//	return _impl->readInTimePoint(time_point);
//}

Reader_ptr dariadb::storage::Engine::readInterval(const QueryInterval &q) {
  return _impl->readInterval(q);
}

Reader_ptr dariadb::storage::Engine::readInTimePoint(const QueryTimePoint &q) {
  return _impl->readInTimePoint(q);
}
