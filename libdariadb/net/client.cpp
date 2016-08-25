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
      _query_num=1;
    _state = ClientState::CONNECT;
    _pings_answers = 0;
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

    _state = ClientState::CONNECT;
    _thread_handler =
        std::move(std::thread{&Client::Private::client_thread, this});

    while(this->state()!= ClientState::WORK){
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
    }
    _state = ClientState::WORK;
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

  void client_thread() {
    ip::tcp::endpoint ep(ip::address::from_string(_params.host), _params.port);
    auto raw_sock_ptr = new ip::tcp::socket(_service);
    _socket = socket_ptr{raw_sock_ptr};
    _socket->async_connect(
        ep, std::bind(&Client::Private::connect_handler, this, _1));
    _service.run();
  }

  void readNext() {
    async_read_until(*_socket.get(), buff, '\n',
                     std::bind(&Client::Private::onRead, this, _1, _2));
  }

  void onDisconnectSended(const boost::system::error_code &err,
                          size_t read_bytes) {
    if (err) {
      THROW_EXCEPTION_SS("server::onDisconnectSended - " << err.message());
    }
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

    this->readNext();
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

    if(msg.size()> OK_ANSWER.size() && msg.substr(0,2)==OK_ANSWER){
        auto query_num = stoi(msg.substr(3,msg.size()));
        logger("client: query #",query_num, " accepted");
        if(this->_state!=ClientState::CONNECT){

        }else{
            this->_state=ClientState::WORK;
        }
    }

    if (msg == DISCONNECT_ANSWER) {
      logger("client: disconnected.");
      _state = ClientState::DISCONNECTED;
      this->_socket->close();
      return;
    }

    if (msg == PING_QUERY) {
      logger("client: ping.");
      async_write(*_socket.get(), buffer(PONG_ANSWER + "\n"),
                  std::bind(&Client::Private::onPongSended, this, _1, _2));
    }

    readNext();
  }

  void onPongSended(const boost::system::error_code &err, size_t read_bytes) {
    if (err) {
      THROW_EXCEPTION_SS("server::onPongSended - " << err.message());
    }
    _pings_answers++;
    logger("client: pong");
  }

  size_t pings_answers() const { return _pings_answers.load(); }
  ClientState state() const { return _state; }

  io_service _service;
  socket_ptr _socket;
  streambuf buff;

  Client::Param _params;
  utils::Locker _locker;
  std::thread _thread_handler;
  ClientState _state;
  std::atomic_size_t _pings_answers;

  std::atomic_int _query_num;
};

Client::Client(const Param &p) : _Impl(new Client::Private(p)) {}

Client::~Client() {}

void Client::connect() { _Impl->connect(); }

void Client::disconnect() { _Impl->disconnect(); }

ClientState Client::state() const { return _Impl->state(); }

size_t Client::pings_answers() const { return _Impl->pings_answers(); }
