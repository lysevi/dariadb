#pragma once

#include "../meas.h"
#include "../storage/query_param.h"
#include "net_common.h"
#include <memory>
#include <string>
namespace dariadb {
namespace net {
class Client {
public:
  struct Param {
    std::string host;
    unsigned short port;
    Param(const std::string &_host, unsigned short _port) {
      host = _host;
      port = _port;
    }
  };
  Client(const Param &p);
  ~Client();

  void connect();
  void disconnect();

  ClientState state() const;
  size_t pings_answers() const;

  /// connection id on server
  int id() const;

  void write(const Meas::MeasArray &ma);
  /*  Meas::MeasList read(const storage::QueryInterval&qi);*/
protected:
  class Private;
  std::unique_ptr<Private> _Impl;
};
}
}
