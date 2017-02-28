#include <libdariadb/dariadb.h>
#include <iostream>

class QuietLogger : public dariadb::utils::ILogger {
public:
  void message(dariadb::utils::LOG_MESSAGE_KIND kind, const std::string &msg) override {}
};

class Callback : public dariadb::IReadCallback {
public:
  Callback() {}

  void apply(const dariadb::Meas &measurement) override {
    std::cout << " id: " << measurement.id
              << " timepoint: " << dariadb::timeutil::to_string(measurement.time)
              << " value:" << measurement.value << std::endl;
  }

  void is_end() override {
    std::cout << "calback end." << std::endl;
    dariadb::IReadCallback::is_end();
  }
};

int main(int, char **) {
  const std::string storage_path = "exampledb";

  // Replace standart logger.
  dariadb::utils::ILogger_ptr log_ptr{new QuietLogger()};
  dariadb::utils::LogManager::start(log_ptr);

  auto storage = dariadb::open_storage(storage_path);
  auto scheme = dariadb::scheme::Scheme::create(storage->settings());

  // we can get param id`s from scheme file
  auto all_params = scheme->ls();
  dariadb::IdArray all_id;
  all_id.reserve(all_params.size());
  all_id.push_back(all_params.idByParam("group.param1"));
  all_id.push_back(all_params.idByParam("group.subgroup.param2"));

  dariadb::Time start_time = dariadb::MIN_TIME;
  dariadb::Time cur_time = dariadb::timeutil::current_time();

  // query writed interval;
  dariadb::QueryInterval qi(all_id, dariadb::Flag(), start_time, cur_time);
  dariadb::MeasList readed_values = storage->readInterval(qi);
  std::cout << "Readed: " << readed_values.size() << std::endl;
  for (auto measurement : readed_values) {
    std::cout << " param: " << all_params[measurement.id]
              << " timepoint: " << dariadb::timeutil::to_string(measurement.time)
              << " value:" << measurement.value << std::endl;
  }

  // callback
  std::cout << "Callback in interval: " << std::endl;
  std::unique_ptr<Callback> callback_ptr{new Callback()};
  storage->foreach (qi, callback_ptr.get());
  callback_ptr->wait();

  { // stat
    auto stat = storage->stat(dariadb::Id(0), start_time, cur_time);
    std::cout << "count: " << stat.count << std::endl;
    std::cout << "time: [" << dariadb::timeutil::to_string(stat.minTime) << " "
              << dariadb::timeutil::to_string(stat.maxTime) << "]" << std::endl;
    std::cout << "val: [" << stat.minValue << " " << stat.maxValue << "]" << std::endl;
    std::cout << "sum: " << stat.sum << std::endl;
  }
}
