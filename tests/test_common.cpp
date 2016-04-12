#include "test_common.h"
#include <flags.h>
#include <utils/utils.h>
#include <utils/exception.h>
#include <thread>

namespace dariadb_test {
#undef NO_DATA
	
	void checkAll(dariadb::Meas::MeasList res,
		std::string msg,
		dariadb::Time from,
		dariadb::Time to, dariadb::Time  step) {

		dariadb::Id id_val(0);
		dariadb::Flag flg_val(0);
		for (auto i = from; i < to; i += step) {
			size_t count = 0;
			for (auto &m : res) {
				if ((m.id == id_val)
					&& ((m.flag == flg_val) || (m.flag == dariadb::Flags::NO_DATA))
					&& ((m.src == flg_val) || (m.src == dariadb::Flags::NO_DATA)))
				{
					count++;
				}
			}
			
			if (count < copies_count) {
				throw MAKE_EXCEPTION("count < copies_count");
			}
			++id_val;
			++flg_val;
		}
	}

	void check_reader_of_all(dariadb::storage::Reader_ptr reader,
		dariadb::Time from,
		dariadb::Time to,
		dariadb::Time  step, size_t id_count,
		size_t total_count, std::string message)
	{
		dariadb::Meas::MeasList all{};
		reader->readAll(&all);
		if (all.size() != total_count) {
			throw MAKE_EXCEPTION("(all.size() != total_count)");
		}
		//TODO reset is sucks
		reader->reset();
		auto readed_ids = reader->getIds();
		if (readed_ids.size() != id_count) {
			throw MAKE_EXCEPTION("(readed_ids.size() != size_t((to - from) / step))");
		}

		checkAll(all, message, from, to, step);
	}

	void storage_test_check(dariadb::storage::BaseStorage *as,
		dariadb::Time from,
		dariadb::Time to,
		dariadb::Time  step, dariadb::Time write_window_size) {
		auto m = dariadb::Meas::empty();
		size_t total_count = 0;

		dariadb::Id id_val(0);
		
		dariadb::Flag flg_val(0);
		
		for (auto i = from; i < to; i += step) {
			m.id = id_val;
			m.flag = flg_val;
			m.src = flg_val;
			m.time = i;
			m.value = 0;
			++id_val;
			++flg_val;
			for (size_t j = 1; j < copies_count + 1; j++) {
				if (as->append(m).writed != 1) {
					throw MAKE_EXCEPTION("->append(m).writed != 1");
				}
				total_count++;
				m.value = dariadb::Value(j);
				m.time++;
			}
		}
        std::this_thread::sleep_for(std::chrono::milliseconds(dariadb::Time(write_window_size)));
		dariadb::Meas::MeasList current_mlist;
		as->currentValue(dariadb::IdArray{}, 0)->readAll(&current_mlist);
		if (current_mlist.size() == 0) {
			throw MAKE_EXCEPTION("current_mlist.size()>0");
		}

		as->flush();

		auto reader = as->readInterval(from, to+ copies_count);
		check_reader_of_all(reader, from, to, step, id_val,total_count, "readAll error: ");

		auto cloned_reader = reader->clone();
		cloned_reader->reset();
		check_reader_of_all(cloned_reader, from, to , step, id_val, total_count, "cloned readAll error: ");

		dariadb::IdArray ids{};
		dariadb::Meas::MeasList all{};
		as->readInterval(ids, 0, from, to + copies_count)->readAll(&all);
		if (all.size() != total_count) {
			throw MAKE_EXCEPTION("all.size() != total_count");
		}

		checkAll(all, "read error: ", from, to, step);

		ids.push_back(2);
		dariadb::Meas::MeasList fltr_res{};
		as->readInterval(ids, 0, from, to + copies_count)->readAll(&fltr_res);

		if (fltr_res.size() != copies_count) {
			throw MAKE_EXCEPTION("fltr_res.size() != copies_count");
		}

		all.clear();
		as->readInTimePoint(to + copies_count)->readAll(&all);
		size_t ids_count = (size_t)((to - from) / step);
		if (all.size() != ids_count) {
			throw MAKE_EXCEPTION("all.size() != ids_count");
		}

		dariadb::IdArray emptyIDs{};
		fltr_res.clear();
		as->readInTimePoint(to + copies_count)->readAll(&fltr_res);
		if (fltr_res.size() != ids_count) {
			throw MAKE_EXCEPTION("fltr_res.size() != ids_count");
		}

		dariadb::IdArray notExstsIDs{ 9999 };
		fltr_res.clear();
		as->readInTimePoint(notExstsIDs, 0, to - 1)->readAll(&fltr_res);
		if (fltr_res.size() != size_t(1)) {
			throw MAKE_EXCEPTION("fltr_res.size() != size_t(1)");
		}

		if (fltr_res.front().flag != dariadb::Flags::NO_DATA) {
			throw MAKE_EXCEPTION("fltr_res.front().flag != dariadb::Flags::NO_DATA");
		}

	}
}
