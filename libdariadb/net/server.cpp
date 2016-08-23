#include "server.h"
#include "../utils/logger.h"
#include "../utils/exception.h"
#include "net_common.h"
#include <atomic>
#include <boost/asio.hpp>
#include <functional>
#include <thread>
#include <sstream>

using namespace std::placeholders;
using namespace boost::asio;

using namespace dariadb;
using namespace dariadb::net;


typedef boost::shared_ptr<ip::tcp::socket> socket_ptr;
typedef boost::shared_ptr<ip::tcp::acceptor> acceptor_ptr;

struct ClientIO{
    socket_ptr sock;
    streambuf buff;

    ClientIO(socket_ptr _sock){
        sock=_sock;
    }

    void readHello(){

    }
};

class Server::Private {
public:
  Private(const Server::Param &p):_params(p),_stop_flag(false),_is_runned_flag(false){
      _connections_accepted.store(0);
      _thread_handler=std::thread(&Server::Private::server_thread, this);
  }

  ~Private(){
      stop();
  }

  void stop(){
      _stop_flag=true;
      _service.stop();
      _thread_handler.join();
  }
  void server_thread(){
      logger_info("server: start server on ", _params.port, "...");


      ip::tcp::endpoint ep(ip::tcp::v4(), _params.port);
      _acc = acceptor_ptr{new ip::tcp::acceptor(_service, ep)};
      socket_ptr sock(new ip::tcp::socket(_service));
      start_accept(sock);
      logger_info("server: OK");
      while(!_stop_flag){
          _service.poll_one();
          _is_runned_flag.store(true);
      }

  }

  void start_accept(socket_ptr sock) {
    _acc->async_accept(*sock, std::bind(&Server::Private::handle_accept, this, sock, _1));
  }

  void handle_accept(socket_ptr sock, const boost::system::error_code &err) {
      if(err){
          THROW_EXCEPTION_SS("dariadb::server: error on accept - "<<err.message());
      }

    logger_info("server: accept connection.");

    char buff[1024];
    auto readed=sock->read_some(buffer(buff,1024));
    if(readed<HELLO_PREFIX.size()){
        logger_fatal("server: bad hello - ", buff);
    }else{
        std::istringstream iss(buff);
        std::string readed_str;
        iss >> readed_str;
        assert(readed_str==HELLO_PREFIX);
        iss >> readed_str;
        logger_info("server: hello from {", readed_str,'}');
        _connections_accepted += 1;
    }
    socket_ptr new_sock(new ip::tcp::socket(_service));
    start_accept(new_sock);
  }

  size_t connections_accepted() const { return _connections_accepted.load(); }

  bool is_runned(){
      return _is_runned_flag.load();
  }

  io_service _service;
  acceptor_ptr _acc;

  std::atomic_size_t _connections_accepted;
  Server::Param _params;
  std::thread _thread_handler;

  std::atomic_bool _stop_flag;
  std::atomic_bool _is_runned_flag;
};

Server::Server(const Param &p) : _Impl(new Server::Private(p)) {}

Server::~Server() {}

bool Server::is_runned(){
  return _Impl->is_runned();
}

size_t Server::connections_accepted() const {
  return _Impl->connections_accepted();
}

void Server::stop(){
    _Impl->stop();
}
