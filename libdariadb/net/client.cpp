#include "client.h"
#include "../utils/logger.h"
#include <boost/asio.hpp>
#include <functional>

using namespace dariadb;
using namespace dariadb::net;
using namespace std::placeholders;

typedef boost::shared_ptr<boost::asio::ip::tcp::socket> socket_ptr;

class Client::Private {
public:
  Private(const Client::Param &p) : _params(p) {}

  void connect() {
    logger_info("client: connecting to ", _params.host, ':', _params.port);

    boost::asio::ip::tcp::endpoint ep(
        boost::asio::ip::address::from_string(_params.host), _params.port);
    auto raw_sock_ptr = new boost::asio::ip::tcp::socket(_service);
    _socket = socket_ptr{raw_sock_ptr};
    _socket->async_connect(ep, std::bind(&Client::Private::connect_handler, this, _1));
    _service.run();
  }

  void connect_handler(const boost::system::error_code &ec) {
      logger_info("client: connectection successful");
  }

  boost::asio::io_service _service;
  socket_ptr _socket;
  Client::Param _params;
};

Client::Client(const Param &p) : _Impl(new Client::Private(p)) {}

Client::~Client() {}

void Client::connect() { _Impl->connect(); }
