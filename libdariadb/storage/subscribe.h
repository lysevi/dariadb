#pragma once

#include <libdariadb/meas.h>
#include <libdariadb/storage/callbacks.h>
#include <libdariadb/utils/async/locker.h>
#include <memory>

namespace dariadb {
namespace storage {

struct SubscribeInfo {
  SubscribeInfo() = default;
  SubscribeInfo(const IdArray &i, const Flag &f, const ReaderCallback_ptr &c);
  IdArray ids;
  Flag flag;
  mutable ReaderCallback_ptr clbk;
  bool isYours(const dariadb::Meas &m) const;
};

typedef std::shared_ptr<SubscribeInfo> SubscribeInfo_ptr;

struct SubscribeNotificator {
  std::list<SubscribeInfo_ptr> _subscribes;
  bool is_stoped;
  std::mutex _locker;

  SubscribeNotificator() = default;
  ~SubscribeNotificator();
  void start();
  void stop();
  void add(const SubscribeInfo_ptr &n);
  // TODO replace Meas to MEasArray and run on thread.
  void on_append(const dariadb::Meas &m) const;
};
}
}
