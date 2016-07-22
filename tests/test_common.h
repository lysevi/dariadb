#pragma once

#include <flags.h>
#include <meas.h>
#include <string>
#include <interfaces/imeasstorage.h>

namespace dariadb_test {
const size_t copies_count = 100;

void checkAll(dariadb::Meas::MeasList res, std::string msg, dariadb::Time from,
              dariadb::Time to, dariadb::Time step);

void storage_test_check(dariadb::storage::IMeasStorage *as, dariadb::Time from,
                        dariadb::Time to, dariadb::Time step);

/*void readIntervalCommonTest(dariadb::storage::MeasStorage *ds);*/
}
