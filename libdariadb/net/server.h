#pragma once

#include "../interfaces/imeasstorage.h"
#include <memory>

namespace dariadb {
namespace net {

const size_t SERVER_IO_THREADS_DEFAULT = 3;

class Server {
public:
  struct Param {
    unsigned short port;
    size_t io_threads;
    Param(unsigned short _port) {
      port = _port;
      io_threads = SERVER_IO_THREADS_DEFAULT;
    }
  };
  Server(const Param &p);
  ~Server();

  void start();
  void stop();
  bool is_runned();
  size_t connections_accepted() const;
  void set_storage(storage::IMeasStorage *storage);

protected:
  class Private;
  std::unique_ptr<Private> _Impl;
};
}
}
