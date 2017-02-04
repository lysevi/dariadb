#include <libdariadb/storage/subscribe.h>
#include <libdariadb/utils/utils.h>
#include <algorithm>

using namespace dariadb::storage;
using namespace dariadb;

SubscribeInfo::SubscribeInfo(const IdArray &i, const Flag &f, const ReaderCallback_ptr &c)
    : ids(i), flag(f), clbk(c) {}

bool SubscribeInfo::isYours(const dariadb::Meas &m) const {
  if ((ids.size() == 0) || (std::count(ids.cbegin(), ids.cend(), m.id))) {
    if ((flag == 0) || (flag == m.flag)) {
      return true;
    }
  }
  return false;
}

SubscribeNotificator::~SubscribeNotificator() {
  if (!is_stoped) {
    this->stop();
  }
}

void SubscribeNotificator::start() {
  std::lock_guard<std::mutex> lg(_locker);
  is_stoped = false;
}

void SubscribeNotificator::stop() {
  std::lock_guard<std::mutex> lg(_locker);
  is_stoped = true;
}

void SubscribeNotificator::add(const SubscribeInfo_ptr &n) {
  std::lock_guard<std::mutex> lg(_locker);
  ENSURE(!is_stoped);
  _subscribes.push_back(n);
}

void SubscribeNotificator::on_append(const dariadb::Meas &m) const {
  for (auto si : _subscribes) {
    ENSURE(si->clbk != nullptr);
    if (si->isYours(m)) {
      si->clbk->apply(m);
    }
  }
}
