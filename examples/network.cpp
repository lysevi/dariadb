#include <libclient/client.h>
#include <iostream>

std::string get_name_by_id(const std::map<std::string, dariadb::Id> &name2id,
                           const dariadb::Id &id) {
  for (auto kv : name2id) {
    if (kv.second == id) {
      return kv.first;
    }
  }
  throw std::logic_error("bad id");
}

int main(int argc, char **argv) {
  const size_t MEASES_SIZE = 99;
  unsigned short server_port = 2001;
  unsigned short server_http_port = 2002;
  std::string server_host = "127.0.0.1";
  dariadb::net::client::Client::Param p(server_host, server_port, server_http_port);

  dariadb::net::client::Client_Ptr connection{new dariadb::net::client::Client(p)};
  connection->connect();

  if (!connection->addToScheme("cpu.core1")) {
    throw std::logic_error("create param error");
  }

  if (!connection->addToScheme("cpu.core2")) {
    throw std::logic_error("create param error");
  }

  if (!connection->addToScheme("memory")) {
    throw std::logic_error("create param error");
  }

  std::map<std::string, dariadb::Id> name2id = connection->loadScheme();

  dariadb::MeasArray ma;
  auto current_time = dariadb::timeutil::current_time();
  for (size_t i = 0; i < MEASES_SIZE; ++i) {
    dariadb::Meas cpu1, cpu2, memory;
    cpu1.id = name2id["cpu.core1"];
    cpu1.value = dariadb::Value(i);
    cpu1.time = current_time++;

    cpu2.id = name2id["cpu.core2"];
    cpu2.value = dariadb::Value(i);
    cpu2.time = current_time++;

    memory.id = name2id["memory"];
    memory.value = dariadb::Value(i);
    memory.time = current_time++;

    ma.push_back(cpu1);
    ma.push_back(cpu2);
    ma.push_back(memory);
  }

  connection->append(ma);

  dariadb::IdArray ids{name2id["cpu.core1"], name2id["cpu.core2"], name2id["memory"]};
  dariadb::QueryInterval interval_query(ids, dariadb::Flag(), dariadb::MIN_TIME,
                                        current_time);
  auto values = connection->readInterval(interval_query);

  for (auto measurement : values) {
    std::cout << " id: " << measurement.id
              << " name:" << get_name_by_id(name2id, measurement.id)
              << " timepoint: " << dariadb::timeutil::to_string(measurement.time)
              << " value:" << measurement.value << std::endl;
  }
  connection->disconnect();
}
