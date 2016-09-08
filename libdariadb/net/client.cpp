#include "client.h"
#include "../utils/exception.h"
#include "../utils/locker.h"
#include "../utils/logger.h"
#include "async_connection.h"
#include "net_common.h"

#include <json/json.hpp>
#define BOOST_ASIO_ENABLE_HANDLER_TRACKING
#include <boost/asio.hpp>
#include <functional>
#include <thread>

using namespace std::placeholders;
using namespace boost::asio;

using namespace dariadb;
using namespace dariadb::net;

typedef boost::shared_ptr<ip::tcp::socket> socket_ptr;

class Client::Private : public AsyncConnection {
public:
	Private(const Client::Param &p) :AsyncConnection(nullptr), _params(p) {
    _query_num = 1;
    _state = ClientState::CONNECT;
    _pings_answers = 0;
	this->set_pool(&_pool);
  }

  ~Private() noexcept(false) {
    try {
      if (_state != ClientState::DISCONNECTED && _socket != nullptr) {
        this->disconnect();
      }
      /*if (_socket->is_open()) {
        boost::system::error_code err;
        _socket->cancel(err);
        if (err) {
          logger("client: #", id(), " on socket::cancel - ", err.message());
        }
      }*/
      _service.stop();
      _thread_handler.join();
    } catch (std::exception &ex) {
      THROW_EXCEPTION_SS("client: #" << id() << ex.what());
    }
  }

  void connect() {
    logger_info("client: connecting to ", _params.host, ':', _params.port);

    _state = ClientState::CONNECT;
    _thread_handler = std::move(std::thread{&Client::Private::client_thread, this});

    while (this->_state != ClientState::WORK) {
      std::this_thread::sleep_for(std::chrono::milliseconds(300));
    }
  }

  void disconnect() {
    if (_socket->is_open()) {
		auto nd = _pool.construct(DataKinds::DISCONNECT);
      this->send(nd);
    }

    while (this->_state != ClientState::DISCONNECTED) {
      logger("client: #", id(), " disconnect - wait server answer...");
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
  }

  void client_thread() {
    ip::tcp::endpoint ep(ip::address::from_string(_params.host), _params.port);
    auto raw_sock_ptr = new ip::tcp::socket(_service);
    _socket = socket_ptr{raw_sock_ptr};
    _socket->async_connect(ep, std::bind(&Client::Private::connect_handler, this, _1));
    _service.run();
  }

  void onNetworkError(const boost::system::error_code &err) override {
    if (this->_state != ClientState::DISCONNECTED) {
      THROW_EXCEPTION_SS("client: #" << id() << err.message());
    }
  }

  void onDataRecv(const NetData_ptr &d, bool&cancel, bool&dont_free_memory) override {
    if (this->_state == ClientState::WORK) {
      logger("client: #", id(), " dataRecv ", d->size, " bytes.");
    } else {
      logger("client: dataRecv ", d->size, " bytes.");
    }

    if (d->data[0] == (uint8_t)DataKinds::OK) {
      auto query_num = *reinterpret_cast<uint32_t*>(&d->data[1]);
      logger("client: #", id(), " query #", query_num, " accepted.");
      if (this->_state != ClientState::WORK) {
        THROW_EXCEPTION_SS("(this->_state != ClientState::WORK)" << this->_state);
      }
      return;
    }

    if (d->data[0] == (uint8_t)DataKinds::PING) {
      logger("client: #", id(), " ping.");
	  auto nd = _pool.construct(DataKinds::PONG);
      this->send(nd);
      _pings_answers++;
      return;
    }

    if (d->data[0] == (uint8_t)DataKinds::DISCONNECT) {
      cancel=true;
      logger("client: #", id(), " disconnection.");
      try {
        _state = ClientState::DISCONNECTED;
        this->full_stop();
        this->_socket->close();
      } catch (...) {
      }
      logger("client: #", id(), " disconnected.");
      return;
    }

    // hello id
    if (d->data[0] == (uint8_t)DataKinds::HELLO) {
      auto id = *reinterpret_cast<int32_t*>(&d->data[1]);
      this->set_id(id);
      this->_state = ClientState::WORK;
      logger("client: #", id, " ready.");
    }
  }

  void connect_handler(const boost::system::error_code &ec) {
    if (ec) {
      THROW_EXCEPTION_SS("dariadb::client: error on connect - " << ec.message());
    }
    this->start(this->_socket);
    std::lock_guard<utils::Locker> lg(_locker);
    auto hn=ip::host_name();
    auto sz=sizeof(DataKinds::HELLO) + hn.size();

    logger("client: send hello ", hn);

	auto nd = _pool.construct();
    nd->size=static_cast<NetData::MessageSize>(sz);
    nd->data[0]=(uint8_t)DataKinds::HELLO;
    memcpy(&nd->data[1],hn.data(),hn.size());
    this->send(nd);
  }


  size_t pings_answers() const { return _pings_answers.load(); }
  ClientState state() const { return _state; }
  
  void write(const Meas::MeasArray &ma) {
    std::lock_guard<utils::Locker> lg(this->_locker);
	logger("client: send ", ma.size());
	size_t writed = 0;
	while (writed!=ma.size()) {
		auto left = (ma.size() - writed);
		
		//first byte for query type
		auto cur_msg_space = (NetData::MAX_MESSAGE_SIZE - 1);
		size_t count_to_write = (left*sizeof(Meas))>cur_msg_space?cur_msg_space/sizeof(Meas):left;
		logger("client: pack count: ", count_to_write);
		
		auto size_to_write = count_to_write * sizeof(Meas); 
		
		auto nd = this->_pool.construct(DataKinds::WRITE);
		memcpy(&nd->data[1], ma.data()+writed, size_to_write);
		nd->size += size_to_write;
		send(nd);
		writed += count_to_write;
	}
  }


  io_service _service;
  socket_ptr _socket;
  streambuf buff;

  Client::Param _params;
  utils::Locker _locker;
  utils::Locker *_query_locker; // lock current query.
  std::thread _thread_handler;
  ClientState _state;
  std::atomic_size_t _pings_answers;

  std::atomic_int _query_num;
  Meas::MeasArray in_buffer_values;
  NetData_Pool _pool;
};

Client::Client(const Param &p) : _Impl(new Client::Private(p)) {}

Client::~Client() {}

int Client::id() const {
  return _Impl->id();
}

void Client::connect() {
  _Impl->connect();
}

void Client::disconnect() {
  _Impl->disconnect();
}

ClientState Client::state() const {
  return _Impl->state();
}

size_t Client::pings_answers() const {
  return _Impl->pings_answers();
}

 void Client::write(const Meas::MeasArray &ma) {
  _Impl->write(ma);
}
//
// Meas::MeasList Client::read(const storage::QueryInterval &qi) {
//  return _Impl->read(qi);
//}
