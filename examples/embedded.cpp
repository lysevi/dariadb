#include <libdariadb/dariadb.h>
#include <libdariadb/utils/fs.h>
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

int main(int argc, char **argv) {
  const std::string storage_path = "exampledb";
  // comment that, for open exists storage.
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }

  // Replace standart logger.
  dariadb::utils::ILogger_ptr log_ptr{new QuietLogger()};
  dariadb::utils::LogManager::start(log_ptr);

  auto storage = dariadb::open_storage(storage_path);
  auto settings = storage->settings();

  auto scheme = dariadb::scheme::Scheme::create(settings);

  auto p1 = scheme->addParam("group.param1");
  auto p2 = scheme->addParam("group.subgroup.param2");
  scheme->save();

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
      std::cerr << "Error: " << dariadb::to_string(status.error) << std::endl;
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

  // current values
  dariadb::Id2Meas cur_values = storage->currentValue(all_id, dariadb::Flag());
  std::cout << "Current: " << std::endl;
  for (auto kv : timepoint) {
    auto measurement = kv.second;
    std::cout << " id: " << all_params[kv.first]
              << " timepoint: " << dariadb::timeutil::to_string(measurement.time)
              << " value:" << measurement.value << std::endl;
  }

  // callback
  std::cout << "Callback in interval: " << std::endl;
  Callback callback;
  storage->foreach (qi, &callback);
  callback.wait();

  // callback
  std::cout << "Callback in timepoint: " << std::endl;
  storage->foreach (qp, &callback);
  callback.wait();

  { // stat
    auto stat = storage->stat(dariadb::Id(0), start_time, m.time);
    std::cout << "count: " << stat.count << std::endl;
    std::cout << "time: [" << dariadb::timeutil::to_string(stat.minTime) << " "
              << dariadb::timeutil::to_string(stat.maxTime) << "]" << std::endl;
    std::cout << "val: [" << stat.minValue << " " << stat.maxValue << "]" << std::endl;
    std::cout << "sum: " << stat.sum << std::endl;
  }

  { // statistical functions
    dariadb::statistic::Calculator calc(storage);
    auto all_functions = dariadb::statistic::FunctionFactory::functions();
    std::cout << "available functions: " << std::endl;
    for (auto fname : all_functions) {
      std::cout << " " << fname << std::endl;
    }
    auto result =
        calc.apply(dariadb::Id(0), start_time, m.time, dariadb::Flag(), all_functions);
    for (size_t i = 0; i < result.size(); ++i) {
      auto measurement = result[i];
      std::cout << all_functions[i] << " id: " << all_params[m.id].name
                << " timepoint: " << dariadb::timeutil::to_string(measurement.time)
                << " value:" << measurement.value << std::endl;
    }
  }
}
