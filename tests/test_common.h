#pragma once

#include <libdariadb/flags.h>
#include <libdariadb/interfaces/imeasstorage.h>
#include <libdariadb/meas.h>
#include <string>

namespace dariadb_test {
const size_t copies_count = 100;

void checkAll(dariadb::MeasList res, std::string msg, dariadb::Time from,
              dariadb::Time to, dariadb::Time step);

void storage_test_check(dariadb::storage::IMeasStorage *as, dariadb::Time from,
                        dariadb::Time to, dariadb::Time step, bool check_stop_flag);

/*void readIntervalCommonTest(dariadb::storage::MeasStorage *ds);*/
}
