#include <libdariadb/dariadb.h>
#include <libdariadb/utils/fs.h>
#include <iostream>

int main(int, char **) {
  const std::string storage_path = "exampledb";
  // remove
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }

  auto settings = dariadb::storage::Settings::create(storage_path);
  settings->save();

  // create named param. p1 and p2 contain id of created timeseries.
  auto scheme = dariadb::scheme::Scheme::create(settings);
  auto p1 = scheme->addParam("group.param1");
  auto p2 = scheme->addParam("group.subgroup.param2");
  scheme->save();

  auto storage = std::make_unique<dariadb::Engine>(settings);

  auto m = dariadb::Meas();
  auto start_time = dariadb::timeutil::current_time();

  // write values in interval [currentTime:currentTime+10]
  m.time = start_time;
  for (size_t i = 0; i < 10; ++i) {
    if (i % 2) {
      m.id = p1;
    } else {
      m.id = p2;
    }

    m.time++;
    m.value++;
    m.flag = 100 + i % 2;
    auto status = storage->append(m);
    if (status.writed != 1) {
      std::cerr << "Error: " << status.error_message << std::endl;
    }
  }
}
