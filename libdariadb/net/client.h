#pragma once

#include "../meas.h"
#include "../storage/query_param.h"
#include "net_common.h"
#include "../utils/locker.h"
#include <memory>
#include <string>
#include <functional>
namespace dariadb {
namespace net {
namespace client {

struct ReadResult {
  using callback = std::function<void(const ReadResult*parent, const Meas&m)>;
  QueryNumber id;
  utils::Locker locker;
  callback clbk;
  bool is_closed;
  void wait() { locker.lock(); }
};
using ReadResult_ptr = std::shared_ptr<ReadResult>;

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
  Meas::MeasList read(const storage::QueryInterval &qi);
  ReadResult_ptr read(const storage::QueryInterval&qi,  ReadResult::callback&clbk);
protected:
  class Private;
  std::unique_ptr<Private> _Impl;
};
}
}
}
