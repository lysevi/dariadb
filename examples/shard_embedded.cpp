#include <libdariadb/engines/shard.h>
#include <libdariadb/scheme/scheme.h>
#include <libdariadb/storage/settings.h>
#include <libdariadb/timeutil.h>
#include <libdariadb/utils/fs.h>
#include <iomanip>
#include <iostream>
#include <sstream>

class QuietLogger : public dariadb::utils::ILogger {
public:
  void message(dariadb::utils::LOG_MESSAGE_KIND kind, const std::string &msg) override {}
};

int main(int argc, char **argv) {
  const std::string storage_path = "exampledb";
  // comment that, for open exists storage.
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
  // Replace standart logger.
  dariadb::utils::ILogger_ptr log_ptr{new QuietLogger()};
  dariadb::utils::LogManager::start(log_ptr);

  // init shards folder with default settings
  auto settings = dariadb::storage::Settings::create(storage_path);
  settings->save();
  auto storage = dariadb::ShardEngine::create(storage_path);
  // init each shards with custom settings
  { // shard for Id==0;
    auto path = dariadb::utils::fs::append_path(storage_path, "shard1");
    auto settings = dariadb::storage::Settings::create(path);
    settings->wal_cache_size.setValue(100);
    settings->wal_file_size.setValue(settings->wal_cache_size.value() * 5);
    settings->chunk_size.setValue(512);
    settings->strategy.setValue(dariadb::STRATEGY::WAL);
    settings->save();
    storage->shardAdd({path, "shard1", {dariadb::Id(0)}});
  }

  { // default shard for all id.
    auto path = dariadb::utils::fs::append_path(storage_path, "shard2");
    auto settings = dariadb::storage::Settings::create(path);
    settings->wal_cache_size.setValue(100);
    settings->wal_file_size.setValue(settings->wal_cache_size.value() * 5);
    settings->strategy.setValue(dariadb::STRATEGY::COMPRESSED);
    settings->save();
    storage->shardAdd({path, "shard2", dariadb::IdSet()});
  }

  auto scheme = dariadb::scheme::Scheme::create(settings);

  auto p1 = scheme->addParam("group.param1");
  auto p2 = scheme->addParam("group.subgroup.param2");

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
  // we can get param id`s from scheme
  auto all_params = scheme->ls();
  dariadb::IdArray all_id;
  all_id.reserve(all_params.size());
  all_id.push_back(all_params.idByParam("group.param1"));
  all_id.push_back(all_params.idByParam("group.subgroup.param2"));

  // query writed interval;
  dariadb::QueryInterval qi(all_id, dariadb::Flag(), start_time, m.time);
  dariadb::MeasArray readed_values = storage->readInterval(qi);
  std::cout << "Readed: " << readed_values.size() << std::endl;
  for (auto measurement : readed_values) {
    std::cout << " param: " << all_params[measurement.id]
              << " timepoint: " << dariadb::timeutil::to_string(measurement.time)
              << " value:" << measurement.value << std::endl;
  }

  // query in timepoint;
  dariadb::QueryTimePoint qp(all_id, dariadb::Flag(), m.time);
  dariadb::Id2Meas timepoint = storage->readTimePoint(qp);
  std::cout << "Timepoint: " << std::endl;
  for (auto kv : timepoint) {
    auto measurement = kv.second;
    std::cout << " param: " << all_params[kv.first]
              << " timepoint: " << dariadb::timeutil::to_string(measurement.time)
              << " value:" << measurement.value << std::endl;
  }
}
