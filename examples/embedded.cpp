#include <iomanip>
#include <iostream>
#include <libdariadb/engine.h>
#include <libdariadb/scheme/scheme.h>
#include <libdariadb/utils/fs.h>
#include <sstream>

class QuietLogger : public dariadb::utils::ILogger {
public:
  void message(dariadb::utils::LOG_MESSAGE_KIND kind,
               const std::string &msg) override {}
};

struct TableMaker : public dariadb::storage::Join::Callback {
  void apply(const dariadb::MeasArray &a) override { table.push_back(a); }
  dariadb::storage::Join::Table table;
};

class Callback : public dariadb::IReadCallback {
public:
  Callback() {}

  void apply(const dariadb::Meas &measurement) override {
    std::cout << " id: " << measurement.id << " timepoint: "
              << dariadb::timeutil::to_string(measurement.time)
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

  auto settings = dariadb::storage::Settings::create(storage_path);
  settings->save();

  auto scheme = dariadb::scheme::Scheme::create(settings);

  auto p1 = scheme->addParam("group.param1");
  auto p2 = scheme->addParam("group.subgroup.param2");

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
  // we can get param id`s from scheme
  auto all_params = scheme->ls();
  dariadb::IdArray all_id;
  all_id.reserve(all_params.size());
  all_id.push_back(all_params.idByParam("group.param1"));
  all_id.push_back(all_params.idByParam("group.subgroup.param2"));

  // query writed interval;
  dariadb::QueryInterval qi(all_id, dariadb::Flag(), start_time, m.time);
  dariadb::MeasList readed_values = storage->readInterval(qi);
  std::cout << "Readed: " << readed_values.size() << std::endl;
  for (auto measurement : readed_values) {
    std::cout << " param: " << all_params[measurement.id] << " timepoint: "
              << dariadb::timeutil::to_string(measurement.time)
              << " value:" << measurement.value << std::endl;
  }

  // query in timepoint;
  dariadb::QueryTimePoint qp(all_id, dariadb::Flag(), m.time);
  dariadb::Id2Meas timepoint = storage->readTimePoint(qp);
  std::cout << "Timepoint: " << std::endl;
  for (auto kv : timepoint) {
    auto measurement = kv.second;
    std::cout << " param: " << all_params[kv.first] << " timepoint: "
              << dariadb::timeutil::to_string(measurement.time)
              << " value:" << measurement.value << std::endl;
  }

  // current values
  dariadb::Id2Meas cur_values = storage->currentValue(all_id, dariadb::Flag());
  std::cout << "Current: " << std::endl;
  for (auto kv : timepoint) {
    auto measurement = kv.second;
    std::cout << " id: " << all_params[kv.first] << " timepoint: "
              << dariadb::timeutil::to_string(measurement.time)
              << " value:" << measurement.value << std::endl;
  }

  // callback
  std::cout << "Callback in interval: " << std::endl;
  std::unique_ptr<Callback> callback_ptr{new Callback()};
  storage->foreach (qi, callback_ptr.get());
  callback_ptr->wait();

  // callback
  std::cout << "Callback in timepoint: " << std::endl;
  storage->foreach (qp, callback_ptr.get());
  callback_ptr->wait();

  // join
  dariadb::QueryInterval qi1(dariadb::IdArray{dariadb::Id(0)}, dariadb::Flag(),
                             start_time, m.time);
  dariadb::QueryInterval qi2(dariadb::IdArray{dariadb::Id(1)}, dariadb::Flag(),
                             start_time, start_time + start_time / 2.0);

  auto table_callback = std::make_unique<TableMaker>();
  storage->join({qi1, qi2}, table_callback.get());

  for (auto row : table_callback->table) {
    std::cout << dariadb::timeutil::to_string(row.front().time) << ":[";
    dariadb::row2stream(std::cout, row);
    std::cout << "]" << std::endl;
  }
  { // stat
    auto stat = storage->stat(dariadb::Id(0), start_time, m.time);
    std::cout << "count: " << stat.count << std::endl;
    std::cout << "time: [" << dariadb::timeutil::to_string(stat.minTime) << " "
              << dariadb::timeutil::to_string(stat.maxTime) << "]" << std::endl;
    std::cout << "val: [" << stat.minValue << " " << stat.maxValue << "]"
              << std::endl;
    std::cout << "sum: " << stat.sum << std::endl;
  }
}
