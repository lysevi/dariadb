#pragma once

#include <memory>
#include "../interfaces/imeasstorage.h"

namespace dariadb {
namespace net {
class Server {
public:
  struct Param {
    unsigned short port;
    Param(unsigned short _port) { port = _port; }
  };
  Server(const Param &p);
  ~Server();

  void stop();
  bool is_runned();
  size_t connections_accepted() const;
  void set_storage(storage::IMeasStorage*storage);
protected:
  class Private;
  std::unique_ptr<Private> _Impl;
};
}
}
