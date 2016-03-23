#include "test_common.h"
#include <boost/test/unit_test.hpp>

namespace dariadb_test {
    void checkAll(dariadb::Meas::MeasList res,
                  std::string msg,
                  dariadb::Time from,
                  dariadb::Time to, dariadb::Time  step) {
        for (auto i = from; i < to; i += step) {
            size_t count = 0;
            for (auto &m : res) {
                if ((m.id == i)
                        && ((m.flag == i) ||(m.flag==dariadb::Flags::NO_DATA))
                        && (m.time == i)
                        && ((m.src == i) ||(m.src==dariadb::Flags::NO_DATA)))
                {
                    count++;
                }

            }
            if (count < copies_count) {
                BOOST_CHECK_EQUAL(copies_count, count);
            }
        }
    }

    void check_reader_of_all(dariadb::storage::Reader_ptr reader,
        dariadb::Time from,
        dariadb::Time to,
        dariadb::Time  step,
        size_t total_count,std::string message)
    {
        dariadb::Meas::MeasList all{};
        reader->readAll(&all);
        BOOST_CHECK_EQUAL(all.size(), total_count);
        auto readed_ids = reader->getIds();
        BOOST_CHECK_EQUAL(readed_ids.size(), size_t(to / step));

        checkAll(all, message, from, to, step);
    }

    void storage_test_check(dariadb::storage::AbstractStorage *as,
                            dariadb::Time from,
                            dariadb::Time to,
                            dariadb::Time  step) {
        auto m = dariadb::Meas::empty();
        size_t total_count = 0;

        for (auto i = from; i < to; i += step) {
            m.id = i;
            m.flag = dariadb::Flag(i);
            m.src = dariadb::Flag(i);
            m.time = i;
            m.value = 0;
            for (size_t j = 1; j < copies_count+1; j++) {
                BOOST_CHECK(as->append(m).writed == 1);
                total_count++;
                m.value = dariadb::Value(j);
            }
        }


        auto reader = as->readInterval(from, to);
        check_reader_of_all(reader, from, to, step, total_count, "readAll error: ");

        auto cloned_reader=reader->clone();
        cloned_reader->reset();
        check_reader_of_all(cloned_reader, from, to, step, total_count, "cloned readAll error: ");

        dariadb::IdArray ids{};
        dariadb::Meas::MeasList all{};
        as->readInterval(ids, 0, from, to)->readAll(&all);
        BOOST_CHECK_EQUAL(all.size(), total_count);

        checkAll(all, "read error: ", from, to, step);

        ids.push_back(from + step);
        dariadb::Meas::MeasList fltr_res{};
        as->readInterval(ids, 0, from, to)->readAll(&fltr_res);

        BOOST_CHECK_EQUAL(fltr_res.size(), copies_count);

        all.clear();
        as->readInTimePoint(to)->readAll(&all);
        size_t ids_count = (size_t)((to - from) / step);
        BOOST_CHECK_EQUAL(all.size(), ids_count);

        dariadb::IdArray emptyIDs{};
        fltr_res.clear();
        as->readInTimePoint(to)->readAll(&fltr_res);
        BOOST_CHECK_EQUAL(fltr_res.size(), ids_count);

        dariadb::IdArray notExstsIDs{9999};
        fltr_res.clear();
        as->readInTimePoint(notExstsIDs,0,to-1)->readAll(&fltr_res);
        BOOST_CHECK_EQUAL(fltr_res.size(), size_t(1));
        BOOST_CHECK_EQUAL(fltr_res.front().flag, dariadb::Flags::NO_DATA);

    }

}
