#include <libdariadb/aggregate/timer.h>
#include <libdariadb/utils/logger.h>
#include <libdariadb/utils/utils.h>

using namespace dariadb;
using namespace dariadb::aggregator;

Timer::Timer() {
  _stop = false;
  _thread = std::thread{std::bind(&Timer::async_loop, this)};
}

Timer::~Timer() {
  logger("aggregation: timer stopping...");
  _stop = true;
  _thread.join();
  logger("aggregation: timer stoped.");
}

void Timer::resort_queue() {
  if (_queue.empty()) {
    return;
  }
  std::sort(_queue.begin(), _queue.end(),
            [](const auto &r, const auto &l) { return l.target > r.target; });
  ENSURE(_queue.front().target <= _queue.back().target);
}

void Timer::addCallback(dariadb::Time firstTime,
                        dariadb::aggregator::ITimer::Callback_Ptr clbk) {
  std::unique_lock<std::mutex> lock(_locker);
  QueueReccord qr;
  qr.target = firstTime;
  qr.clbk = clbk;
  _queue.push_back(qr);
  resort_queue();
}

void Timer::async_loop() {
  while (!_stop) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    std::unique_lock<std::mutex> lock(_locker);
    auto ct = currentTime();
    for (auto it = _queue.begin(); it != _queue.end(); ++it) {
      if (it->target <= ct) {
        it->target = it->clbk->apply(it->target);
      }
    }
    resort_queue();
  }
}