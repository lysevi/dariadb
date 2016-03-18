#include "subscribe.h"
#include <cassert>
#include <algorithm>

using namespace dariadb::storage;

bool SubscribeInfo::isYours(const dariadb::Meas&m) const { 
	if ((ids.size() == 0) || (std::find(ids.cbegin(), ids.cend(), m.id) != ids.cend())) {
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
	std::lock_guard<std::mutex> lg(_mutex);
	is_stoped = false;
}

void SubscribeNotificator::stop() {
	std::lock_guard<std::mutex> lg(_mutex);
	is_stoped = true;
}


void SubscribeNotificator::add(const SubscribeInfo_ptr&n) {
	std::lock_guard<std::mutex> lg(_mutex);
	assert(!is_stoped);
	_subscribes.push_back(n);
}

void SubscribeNotificator::on_append(const dariadb::Meas&m)const {
	for (auto si : _subscribes) {
		assert(si->clbk != nullptr);
		if (si->isYours(m)) {
			si->clbk->call(m);
		}
	}
}