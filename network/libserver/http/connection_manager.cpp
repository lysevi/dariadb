#include <libserver/http/connection_manager.h>

using namespace dariadb::net::http;

connection_manager::connection_manager() {}

void connection_manager::start(connection_ptr c) {
  std::lock_guard<std::mutex> lg(_locker);
  connections_.insert(c);
  c->start();
}

void connection_manager::stop(connection_ptr c) {
  std::lock_guard<std::mutex> lg(_locker);
  connections_.erase(c);
  c->stop();
}

void connection_manager::stop_all() {
  std::lock_guard<std::mutex> lg(_locker);
  for (auto c : connections_) {
    c->stop();
  }
  connections_.clear();
}
