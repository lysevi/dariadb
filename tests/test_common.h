#pragma once

#include <libdariadb/flags.h>
#include <libdariadb/interfaces/imeasstorage.h>
#include <libdariadb/interfaces/ireader.h>
#include <libdariadb/meas.h>
#include <string>

namespace dariadb_test {
const size_t copies_count = 100;

size_t fill_storage_for_test(dariadb::storage::IMeasStorage *as,
                             dariadb::Time from, dariadb::Time to,
                             dariadb::Time step, dariadb::IdSet *_all_ids_set,
                             dariadb::Time *maxWritedTime);
void checkAll(dariadb::MeasList res, std::string msg, dariadb::Time from,
              dariadb::Time to, dariadb::Time step);

void storage_test_check(dariadb::storage::IMeasStorage *as, dariadb::Time from,
                        dariadb::Time to, dariadb::Time step,
                        bool check_stop_flag);

void check_reader(const dariadb::Reader_Ptr &rdr);
/*void readIntervalCommonTest(dariadb::storage::MeasStorage *ds);*/
}
