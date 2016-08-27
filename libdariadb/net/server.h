#pragma once

#include <memory>

namespace dariadb {
namespace net {
class Server {
public:
  struct Param {
    int port;
    Param(int _port) { port = _port; }
  };
  Server(const Param &p);
  ~Server();

  void stop();
  bool is_runned();
  size_t connections_accepted() const;

protected:
  class Private;
  std::unique_ptr<Private> _Impl;
};
}
}
