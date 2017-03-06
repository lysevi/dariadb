#include <libdariadb/dariadb.h>
#include <iostream>

void print_measurement(dariadb::Meas &measurement,
                       dariadb::scheme::DescriptionMap &dmap) {
  std::cout << " param: " << dmap[measurement.id]
            << " timepoint: " << dariadb::timeutil::to_string(measurement.time)
            << " value:" << measurement.value << std::endl;
}

class QuietLogger : public dariadb::utils::ILogger {
public:
  void message(dariadb::utils::LOG_MESSAGE_KIND kind, const std::string &msg) override {}
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

  dariadb::Time cur_time = dariadb::timeutil::current_time();

  // query in timepoint;
  dariadb::QueryTimePoint qp(all_id, dariadb::Flag(), cur_time);
  dariadb::Id2Meas timepoint = storage->readTimePoint(qp);
  std::cout << "Timepoint: " << std::endl;
  for (auto kv : timepoint) {
    auto measurement = kv.second;
    print_measurement(measurement, all_params);
  }

  // current values
  dariadb::Id2Meas cur_values = storage->currentValue(all_id, dariadb::Flag());
  std::cout << "Current: " << std::endl;
  for (auto kv : timepoint) {
    auto measurement = kv.second;
    print_measurement(measurement, all_params);
  }

  // callback
  std::cout << "Callback in timepoint: " << std::endl;
  auto f= dariadb::storage::FunctorCallback::FunctorType([](const dariadb::Meas &m) {
	  std::cout << " id: " << m.id << " timepoint: " << dariadb::timeutil::to_string(m.time)
		  << " value:" << m.value << std::endl;
	  return false;
  });
  dariadb::storage::FunctorCallback callback(f);
  storage->foreach (qp, &callback);
  callback.wait();
}
