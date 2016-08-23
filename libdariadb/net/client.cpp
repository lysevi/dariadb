#include "client.h"
#include "net_common.h"
#include "../utils/logger.h"
#include "../utils/locker.h"
#include <boost/asio.hpp>
#include <functional>

using namespace std::placeholders;
using namespace boost::asio;

using namespace dariadb;
using namespace dariadb::net;

typedef boost::shared_ptr<ip::tcp::socket> socket_ptr;

class Client::Private {
public:
  Private(const Client::Param &p) : _params(p) {}

  ~Private(){
      _socket->shutdown(ip::tcp::socket::shutdown_both);
      _socket->close();
  }

  void connect() {
    logger_info("client: connecting to ", _params.host, ':', _params.port);

    ip::tcp::endpoint ep(
        ip::address::from_string(_params.host), _params.port);
    auto raw_sock_ptr = new ip::tcp::socket(_service);
    _socket = socket_ptr{raw_sock_ptr};
    _socket->async_connect(ep, std::bind(&Client::Private::connect_handler, this, _1));
    _service.run();
  }

  void connect_handler(const boost::system::error_code &ec) {
      logger_info("client: connectection successful");
      std::lock_guard<utils::Locker> lg(_locker);
      std::stringstream ss;
      ss<<HELLO_PREFIX<<' ' <<ip::host_name()<<'\n';
      auto hello_message=ss.str();
      _socket->write_some(buffer(hello_message));
  }

  io_service _service;
  socket_ptr _socket;
  Client::Param _params;
  utils::Locker _locker;
};

Client::Client(const Param &p) : _Impl(new Client::Private(p)) {}

Client::~Client() {}

void Client::connect() { _Impl->connect(); }
