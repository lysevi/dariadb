#include "client.h"
#include "../utils/exception.h"
#include "../utils/locker.h"
#include "../utils/logger.h"
#include "net_common.h"
#include <boost/asio.hpp>
#include <functional>
#include <thread>

using namespace std::placeholders;
using namespace boost::asio;

using namespace dariadb;
using namespace dariadb::net;

typedef boost::shared_ptr<ip::tcp::socket> socket_ptr;

class Client::Private {
public:
  Private(const Client::Param &p) : _params(p) {
    _state = ClientState::CONNECT;
  }

  ~Private() noexcept(false) {
    try {
      if (_state != ClientState::DISCONNECTED && _socket != nullptr) {
        this->disconnect();
      }
      _service.stop();
      _thread_handler.join();
    } catch (std::exception &ex) {
      THROW_EXCEPTION_SS("~Client: " << ex.what());
    }
  }

  void connect() {
    logger_info("client: connecting to ", _params.host, ':', _params.port);

    _thread_handler =
        std::move(std::thread{&Client::Private::client_thread, this});
  }

  void client_thread() {
    ip::tcp::endpoint ep(ip::address::from_string(_params.host), _params.port);
    auto raw_sock_ptr = new ip::tcp::socket(_service);
    _socket = socket_ptr{raw_sock_ptr};
    _socket->async_connect(
        ep, std::bind(&Client::Private::connect_handler, this, _1));
    _service.run();
  }

  void disconnect() {
    std::lock_guard<utils::Locker> lg(_locker);
    std::stringstream ss;
    ss << DISCONNECT_PREFIX << '\n';
    auto bye_message = ss.str();

    async_write(*_socket.get(), buffer(bye_message),
                std::bind(&Client::Private::onDisconnectSended, this, _1, _2));

    while (this->_state != ClientState::DISCONNECTED) {
        logger("client: wait server answer");
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
  }

  void onDisconnectSended(const boost::system::error_code &err,
                          size_t read_bytes) {
    logger("client: send bye");
  }

  void connect_handler(const boost::system::error_code &ec) {
    if (ec) {
      THROW_EXCEPTION_SS("dariadb::client: error on connect - "
                         << ec.message());
    }
    std::lock_guard<utils::Locker> lg(_locker);
    std::stringstream ss;
    ss << HELLO_PREFIX << ' ' << ip::host_name() << '\n';
    auto hello_message = ss.str();
    logger("client: send hello ",
           hello_message.substr(0, hello_message.size() - 1));
    _socket->write_some(buffer(hello_message));

    read_ok("client: no ok answer onConnect - ");
    _state = ClientState::WORK;
    this->readNext();
  }

  void readNext() {
    async_read_until(*_socket.get(), buff, '\n',
                     std::bind(&Client::Private::onRead, this, _1, _2));
  }

  void onRead(const boost::system::error_code &err, size_t read_bytes) {
    logger("client: onRead...");
    if (err) {
      THROW_EXCEPTION_SS("client:  " << err.message());
    }

    std::istream iss(&this->buff);
    std::string msg;
    std::getline(iss, msg);
    logger("client: {", msg, "} readed_bytes: ", read_bytes);

    if (msg == DISCONNECT_ANSWER) {
      logger("client: disconnected.");
      _state = ClientState::DISCONNECTED;
      this->_socket->close();
      return;
    }

    readNext();
  }

  void read_ok(const std::string &message_on_err) {
    streambuf buf;
    read_until(*(_socket.get()), buf, '\n');
    std::istream iss(&buf);
    std::string msg;
    std::getline(iss, msg);

    if (msg != OK_ANSWER) {
      THROW_EXCEPTION_SS(message_on_err << msg);
    } else {
      logger("client: OK.");
    }
  }

  ClientState state()const{
      return _state;
  }

  io_service _service;
  socket_ptr _socket;
  streambuf buff;

  Client::Param _params;
  utils::Locker _locker;
  std::thread _thread_handler;
  ClientState _state;
};

Client::Client(const Param &p) : _Impl(new Client::Private(p)) {}

Client::~Client() {}

void Client::connect() { _Impl->connect(); }

void Client::disconnect() { _Impl->disconnect(); }

ClientState Client::state()const{return _Impl->state(); }
