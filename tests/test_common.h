#pragma once

#include <libdariadb/flags.h>
#include <libdariadb/interfaces/imeasstorage.h>
#include <libdariadb/interfaces/icursor.h>
#include <libdariadb/meas.h>
#include <string>

namespace dariadb_test {
const size_t copies_count = 100;

size_t fill_storage_for_test(dariadb::IMeasStorage *as,
                             dariadb::Time from, dariadb::Time to,
                             dariadb::Time step, dariadb::IdSet *_all_ids_set,
                             dariadb::Time *maxWritedTime, bool random_timestamps);
void checkAll(dariadb::MeasList res, std::string msg, dariadb::Time from,
              dariadb::Time to, dariadb::Time step);

void storage_test_check(dariadb::IMeasStorage *as, dariadb::Time from,
                        dariadb::Time to, dariadb::Time step,
                        bool check_stop_flag, bool random_timestamps=false);

void check_reader(const dariadb::Cursor_Ptr &rdr);
/*void readIntervalCommonTest(dariadb::storage::MeasStorage *ds);*/
}
