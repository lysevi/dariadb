#include "server.h"
#include "../utils/logger.h"
#include <atomic>
#include <boost/asio.hpp>
#include <functional>
#include <thread>

using namespace dariadb;
using namespace dariadb::net;

using namespace std::placeholders;

typedef boost::shared_ptr<boost::asio::ip::tcp::socket> socket_ptr;
typedef boost::shared_ptr<boost::asio::ip::tcp::acceptor> acceptor_ptr;

class Server::Private {
public:
  Private(const Server::Param &p):_params(p),_stop_flag(false){
      _connections_accepted.store(0);
      _thread_handler=std::move(std::thread{server_thread, this});
  }

  ~Private(){
      _stop_flag=true;
      _service.stop();
      _thread_handler.join();
  }

  void server_thread(){
      logger_info("server: start server on ", _params.port);


      boost::asio::ip::tcp::endpoint ep(boost::asio::ip::tcp::v4(), _params.port);
      _acc = acceptor_ptr{new boost::asio::ip::tcp::acceptor(_service, ep)};
      socket_ptr sock(new boost::asio::ip::tcp::socket(_service));
      start_accept(sock);
      _service.run();
  }

  void start_accept(socket_ptr sock) {
    _acc->async_accept(*sock, std::bind(handle_accept, this, sock, _1));
  }

  void handle_accept(socket_ptr sock, const boost::system::error_code &err) {
    if (err)
      return;
    logger_info("server: accept connection.");
    _connections_accepted += 1;
    start_accept(sock);
  }

  size_t connections_accepted() const { return _connections_accepted.load(); }

  boost::asio::io_service _service;
  acceptor_ptr _acc;

  std::atomic_size_t _connections_accepted;
  Server::Param _params;
  std::thread _thread_handler;
  bool _stop_flag;
};

Server::Server(const Param &p) : _Impl(new Server::Private(p)) {}

Server::~Server() {}

size_t Server::connections_accepted() const {
  return _Impl->connections_accepted();
}
