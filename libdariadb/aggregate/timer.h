#pragma once
#include <libdariadb/aggregate/itimer.h>
#include <libdariadb/timeutil.h>
#include <mutex>
#include <queue>
#include <thread>

namespace dariadb {
namespace aggregator {
class Timer : public ITimer {
public:
  struct QueueReccord {
    Time target;
    Callback_Ptr clbk;
    bool operator==(const QueueReccord &other) const { return target == other.target; }

    bool operator<(const QueueReccord &other) const { return target < other.target; }

    bool operator>(const QueueReccord &other) const { return target > other.target; }
  };

  EXPORT Timer();
  EXPORT ~Timer();
  EXPORT void addCallback(dariadb::Time firstTime,
                          dariadb::aggregator::ITimer::Callback_Ptr clbk) override;

  dariadb::Time currentTime() const override { return timeutil::current_time(); }

protected:
  void async_loop();
  void resort_queue();

private:
  std::vector<QueueReccord> _queue;
  std::mutex _locker;
  std::thread _thread;
  bool _stop;
};
} // namespace aggregator
} // namespace dariadb