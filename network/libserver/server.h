#pragma once

#include <libdariadb/engine.h>
#include <libdariadb/interfaces/imeasstorage.h>
#include <libserver/dariadb_net_srv_exports.h>
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
  DARIADBNET_SERVER_EXPORTS Server(const Param &p);
  DARIADBNET_SERVER_EXPORTS ~Server();

  DARIADBNET_SERVER_EXPORTS void start();
  DARIADBNET_SERVER_EXPORTS void stop();
  DARIADBNET_SERVER_EXPORTS bool is_runned();
  DARIADBNET_SERVER_EXPORTS size_t connections_accepted() const;
  DARIADBNET_SERVER_EXPORTS void set_storage(storage::Engine *storage);

  DARIADBNET_SERVER_EXPORTS void asio_run();
protected:
  class Private;
  std::unique_ptr<Private> _Impl;
};
}
}
