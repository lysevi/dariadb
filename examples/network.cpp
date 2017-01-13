#include <libclient/client.h>

int main(int argc, char **argv) {
  const size_t MEASES_SIZE = 200000;
  unsigned short server_port = 2001;
  std::string server_host = "127.0.0.1";
  dariadb::net::client::Client::Param p(server_host, server_port);

  dariadb::net::client::Client_Ptr connection{
      new dariadb::net::client::Client(p)};
  connection->connect();

  dariadb::MeasArray ma;
  ma.resize(MEASES_SIZE);

  auto current_time = dariadb::timeutil::current_time();
  for (size_t i = 0; i < MEASES_SIZE; ++i) {
    ma[i].id = dariadb::Id(1);
    ma[i].value = dariadb::Value(i);
    ma[i].time = current_time + i;
  }

  connection->append(ma);
  connection->disconnect();
}
