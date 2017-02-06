#include <iomanip>
#include <iostream>
#include <libdariadb/engine.h>
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

class Callback : public dariadb::storage::IReadCallback {
public:
  Callback() {}

  void apply(const dariadb::Meas &measurement) override {
    std::cout << " id: " << measurement.id << " timepoint: "
              << dariadb::timeutil::to_string(measurement.time)
              << " value:" << measurement.value << std::endl;
  }

  void is_end() override {
    std::cout << "calback end." << std::endl;
    dariadb::storage::IReadCallback::is_end();
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

  auto storage = std::make_unique<dariadb::storage::Engine>(settings);

  auto m = dariadb::Meas();
  auto start_time = dariadb::timeutil::current_time();

  // write values in interval [currentTime:currentTime+10]
  m.time = start_time;
  for (size_t i = 0; i < 10; ++i) {
    m.id = i % 2;
    m.time++;
    m.value++;
	m.flag = 100+i % 2;
    auto status = storage->append(m);
    if (status.writed != 1) {
      std::cerr << "Error: " << status.error_message << std::endl;
    }
  }

  // query writed interval;
  dariadb::storage::QueryInterval qi(
      dariadb::IdArray{dariadb::Id(0), dariadb::Id(1)}, dariadb::Flag(),
      start_time, m.time);
  dariadb::MeasList readed_values = storage->readInterval(qi);
  std::cout << "Readed: " << readed_values.size() << std::endl;
  for (auto measurement : readed_values) {
    std::cout << " id: " << measurement.id << " timepoint: "
              << dariadb::timeutil::to_string(measurement.time)
              << " value:" << measurement.value << std::endl;
  }

  // query in timepoint;
  dariadb::storage::QueryTimePoint qp(
      dariadb::IdArray{dariadb::Id(0), dariadb::Id(1)}, dariadb::Flag(),
      m.time);
  dariadb::Id2Meas timepoint = storage->readTimePoint(qp);
  std::cout << "Timepoint: " << std::endl;
  for (auto kv : timepoint) {
    auto measurement = kv.second;
    std::cout << " id: " << kv.first << " timepoint: "
              << dariadb::timeutil::to_string(measurement.time)
              << " value:" << measurement.value << std::endl;
  }

  // current values
  dariadb::Id2Meas cur_values = storage->currentValue(
      dariadb::IdArray{dariadb::Id(0), dariadb::Id(1)}, dariadb::Flag());
  std::cout << "Current: " << std::endl;
  for (auto kv : timepoint) {
    auto measurement = kv.second;
    std::cout << " id: " << kv.first << " timepoint: "
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
  dariadb::storage::QueryInterval qi1(dariadb::IdArray{dariadb::Id(0)},
                                      dariadb::Flag(), start_time, m.time);
  dariadb::storage::QueryInterval qi2(dariadb::IdArray{dariadb::Id(1)},
                                      dariadb::Flag(), start_time,
                                      start_time + start_time / 2.0);

  auto table_callback = std::make_unique<TableMaker>();
  storage->join({qi1, qi2}, table_callback.get());

  for (auto row : table_callback->table) {
    std::cout << dariadb::timeutil::to_string(row.front().time) << ":[";
	dariadb::storage::row2stream(std::cout, row);
    std::cout <<"]"<< std::endl;
  }
}
