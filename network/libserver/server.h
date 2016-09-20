#pragma once

#include <libdariadb/engine.h>
#include <libdariadb/interfaces/imeasstorage.h>
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
  Server(const Param &p);
  ~Server();

  void start();
  void stop();
  bool is_runned();
  size_t connections_accepted() const;
  void set_storage(storage::Engine *storage);

  void asio_run();
protected:
  class Private;
  std::unique_ptr<Private> _Impl;
};
}
}
