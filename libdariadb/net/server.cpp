#include "server.h"
#include "../utils/exception.h"
#include "../utils/logger.h"
#include "interfaces/iclientmanager.h"
#include "net_common.h"
#include "server/ioclient.h"
#include <atomic>
#include <boost/asio.hpp>
#include <functional>
#include <istream>
#include <list>
#include <sstream>
#include <thread>
#include <unordered_map>

using namespace std::placeholders;
using namespace boost::asio;

using namespace dariadb;
using namespace dariadb::net;

typedef boost::shared_ptr<ip::tcp::acceptor> acceptor_ptr;

const int PING_TIMER_INTERVAL = 1000;
const int MAX_MISSED_PINGS = 100;

class Server::Private : public IClientManager {
public:
  Private(const Server::Param &p)
      : _params(p), _stop_flag(false), _is_runned_flag(false),
        _ping_timer(_service) {
    _next_id = 0;
    _connections_accepted.store(0);
    _thread_handler = std::thread(&Server::Private::server_thread, this);
    reset_ping_timer();
  }

  ~Private() { stop(); }

  void set_storage(storage::IMeasStorage*storage) {
	  logger("server: set setorage.");
	  _storage = storage;
  }

  void stop() {
    logger("server: stopping...");
    _ping_timer.cancel();
    disconnect_all();
    _stop_flag = true;
    _service.stop();
    _thread_handler.join();
  }

  void disconnect_all() {
    _clients_locker.lock();
    for (auto &kv : _clients) {
      kv.second->disconnect();
    }
    _clients_locker.unlock();
  }

  void server_thread() {
    logger_info("server: start server on ", _params.port, "...");

    ip::tcp::endpoint ep(ip::tcp::v4(), _params.port);
    _acc = acceptor_ptr{new ip::tcp::acceptor(_service, ep)};
    socket_ptr sock(new ip::tcp::socket(_service));
    start_accept(sock);
    logger_info("server: OK");
    while (!_stop_flag) {
      _service.poll_one();
      _is_runned_flag.store(true);
    }
    _is_runned_flag.store(false);
  }

  void start_accept(socket_ptr sock) {
    _acc->async_accept(
        *sock, std::bind(&Server::Private::handle_accept, this, sock, _1));
  }

  void handle_accept(socket_ptr sock, const boost::system::error_code &err) {
    if (err) {
      THROW_EXCEPTION_SS("dariadb::server: error on accept - "
                         << err.message());
    }

    logger_info("server: accept connection.");

    auto cur_id = _next_id.load();
    _next_id++;
    ClientIO_ptr new_client{new ClientIO(cur_id, sock, this, this->_storage)};
    new_client->readHello();

    _clients_locker.lock();
    _clients.insert(std::make_pair(new_client->id, new_client));
    _clients_locker.unlock();

    socket_ptr new_sock(new ip::tcp::socket(_service));
    start_accept(new_sock);
  }

  size_t connections_accepted() const { return _connections_accepted.load(); }

  bool is_runned() { return _is_runned_flag.load(); }

  void client_connect(int id) override {
    std::lock_guard<utils::Locker> lg(_clients_locker);
    auto fres_it = this->_clients.find(id);
    if (fres_it == this->_clients.end()) {
      THROW_EXCEPTION_SS("server: client_connect - client #" << id
                                                             << " not found");
    }
    auto client = fres_it->second;
    _connections_accepted += 1;
    logger_info("server: hello from {", client->host, "}, #", client->id);
    client->state = ClientState::WORK;
  }

  void client_disconnect(int id) override {
    _clients_locker.lock();
    auto fres = _clients.find(id);
    if (fres == _clients.end()) {
      THROW_EXCEPTION_SS("server: client_disconnect - client #"
                         << id << " not found");
    }
    _clients.erase(fres);
    _connections_accepted -= 1;
    logger_info("server: clients count  ", _clients.size(), " accepted:",
                _connections_accepted.load());
    _clients_locker.unlock();
  }

  void reset_ping_timer() {
    try {
      _ping_timer.expires_from_now(
          boost::posix_time::millisec(PING_TIMER_INTERVAL));
      _ping_timer.async_wait(std::bind(&Server::Private::on_check_ping, this));
    } catch (std::exception &ex) {
      THROW_EXCEPTION_SS("server: reset_ping_timer - " << ex.what());
    }
  }

  void on_check_ping() {
    logger_info("server: ping all.");
    std::list<int> to_remove;
    // MAX_MISSED_PINGS
    _clients_locker.lock();
    for (auto &kv : _clients) {
      if (kv.second->pings_missed > MAX_MISSED_PINGS) {
        kv.second->state = ClientState::DISCONNECTED;
        to_remove.push_back(kv.first);
      } else {
        kv.second->ping();
      }
    }
    _clients_locker.unlock();

    _clients_locker.lock();
    for (auto &id : to_remove) {
      logger("server: remove as missed ping #", id);
      _clients[id]->sock->close();
      _clients.erase(id);
    }
    _clients_locker.unlock();
    reset_ping_timer();
  }

  io_service _service;
  acceptor_ptr _acc;

  std::atomic_int _next_id;
  std::atomic_size_t _connections_accepted;
  Server::Param _params;
  std::thread _thread_handler;

  std::atomic_bool _stop_flag;
  std::atomic_bool _is_runned_flag;

  std::unordered_map<int, ClientIO_ptr> _clients;
  utils::Locker _clients_locker;

  deadline_timer _ping_timer;

  storage::IMeasStorage*_storage;
};

Server::Server(const Param &p) : _Impl(new Server::Private(p)) {}

Server::~Server() {}

bool Server::is_runned() { return _Impl->is_runned(); }

size_t Server::connections_accepted() const {
  return _Impl->connections_accepted();
}

void Server::stop() { _Impl->stop(); }

void Server::set_storage(storage::IMeasStorage*storage) {
	_Impl->set_storage(storage);
}