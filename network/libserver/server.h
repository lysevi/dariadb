#pragma once

#include <libdariadb/engine.h>
#include <libdariadb/interfaces/imeasstorage.h>
#include <libserver/net_srv_exports.h>
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

    Param(unsigned short _port, size_t io_threads_count) {
      port = _port;
      io_threads = io_threads_count;
    }
  };
  SRV_EXPORT Server(const Param &p);
  SRV_EXPORT ~Server();

  SRV_EXPORT void start();
  SRV_EXPORT void stop();
  SRV_EXPORT bool is_runned();
  SRV_EXPORT size_t connections_accepted() const;
  SRV_EXPORT void set_storage(Engine *storage);

  SRV_EXPORT void asio_run();

protected:
  class Private;
  std::unique_ptr<Private> _Impl;
};
}
}
