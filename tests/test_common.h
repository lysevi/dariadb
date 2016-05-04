#pragma once

#include <flags.h>
#include <meas.h>
#include <storage/memstorage.h>
#include <string>

namespace dariadb_test {
const size_t sizeInMb10 = 10 * 1024 * 1024;
const size_t copies_count = 100;

void checkAll(dariadb::Meas::MeasList res, std::string msg, dariadb::Time from,
              dariadb::Time to, dariadb::Time step);

void check_reader_of_all(dariadb::storage::Reader_ptr reader,
                         dariadb::Time from, dariadb::Time to,
                         dariadb::Time step, size_t id_count,
                         size_t total_count, std::string message);

void storage_test_check(dariadb::storage::BaseStorage *as, dariadb::Time from,
                        dariadb::Time to, dariadb::Time step,
                        dariadb::Time write_window_size = 0);
}
