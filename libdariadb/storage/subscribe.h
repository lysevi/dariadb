#pragma once

#include "../meas.h"
#include "../storage.h"
#include "../utils/locker.h"
#include <memory>

namespace dariadb {
	namespace storage {

		struct SubscribeInfo {
            SubscribeInfo()=default;
            SubscribeInfo(const IdArray &i,const Flag& f, const ReaderClb_ptr &c);
			IdArray ids;
			Flag flag;
			mutable ReaderClb_ptr clbk;
			bool isYours(const dariadb::Meas&m) const;
		};

		typedef std::shared_ptr<SubscribeInfo> SubscribeInfo_ptr;

		struct SubscribeNotificator {
			std::list<SubscribeInfo_ptr> _subscribes;
			bool is_stoped;
            dariadb::utils::Locker _locker;

			SubscribeNotificator() = default;
			~SubscribeNotificator();
			//TODO must work in thread;
			void start();
			void stop();
			void add(const SubscribeInfo_ptr&n);
			void on_append(const dariadb::Meas&m)const;
		};
	}
}
