#include "client.h"
#include "net_common.h"
#include "../utils/logger.h"
#include "../utils/locker.h"
#include "../utils/exception.h"
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
  Private(const Client::Param &p) : _params(p) {}

  ~Private(){
      _socket->shutdown(ip::tcp::socket::shutdown_both);
      _socket->close();
      _service.stop();
      _thread_handler.join();
  }

  void connect() {
    logger_info("client: connecting to ", _params.host, ':', _params.port);

    _thread_handler=std::move(std::thread{&Client::Private::client_thread,this});
  }

  void client_thread(){
      ip::tcp::endpoint ep(
          ip::address::from_string(_params.host), _params.port);
      auto raw_sock_ptr = new ip::tcp::socket(_service);
      _socket = socket_ptr{raw_sock_ptr};
      _socket->async_connect(ep, std::bind(&Client::Private::connect_handler, this, _1));
      _service.run();
  }

  void disconnect(){
      std::lock_guard<utils::Locker> lg(_locker);
      std::stringstream ss;
      ss<<DISCONNECT_PREFIX<<'\n';
      auto bye_message=ss.str();
      logger("client: send bye");
      _socket->write_some(buffer(bye_message));

      read_ok("client: no ok answer onConnect - ");
  }

  void connect_handler(const boost::system::error_code &ec) {
      if(ec){
          THROW_EXCEPTION_SS("dariadb::client: error on connect - "<<ec.message());
      }
      std::lock_guard<utils::Locker> lg(_locker);
      std::stringstream ss;
      ss<<HELLO_PREFIX<<' ' <<ip::host_name()<<'\n';
      auto hello_message=ss.str();
      logger("client: send hello ",hello_message.substr(0,hello_message.size()-1));
      _socket->write_some(buffer(hello_message));

      read_ok("client: no ok answer onConnect - ");
  }

  void read_ok(const std::string&message_on_err){
      streambuf buf;
      read_until(*(_socket.get()), buf, '\n');
      std::istream iss(&buf);
      std::string msg;
      std::getline(iss,msg);

      if(msg!=OK_ANSWER){
          THROW_EXCEPTION_SS(message_on_err<<msg);
      }else{
          logger("client: OK.");
      }
  }

  io_service _service;
  socket_ptr _socket;
  Client::Param _params;
  utils::Locker _locker;
  std::thread  _thread_handler;
};

Client::Client(const Param &p) : _Impl(new Client::Private(p)) {}

Client::~Client() {}

void Client::connect() { _Impl->connect(); }

void Client::disconnect() {_Impl->disconnect();}
