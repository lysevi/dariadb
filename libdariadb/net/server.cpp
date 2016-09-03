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
      : _params(p), _stop_flag(false), _is_runned_flag(false), _ping_timer(_service) {
    _in_stop_logic = false;
    _next_client_id = 1;
    _connections_accepted.store(0);
    _writes_in_progress.store(0);
  }

  ~Private() { stop(); }

  void set_storage(storage::IMeasStorage *storage) {
    logger("server: set setorage.");
    _storage = storage;
  }

  void stop() {
    _in_stop_logic = true;
    logger_info("server: *** stopping ***");
    while (_writes_in_progress.load() != 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(300));
      logger_info("server: writes in progress ", _writes_in_progress.load());
    }
    disconnect_all();
    _ping_timer.cancel();
    _stop_flag = true;

	logger_info("server: stop asio service.");
    _service.stop();
    logger_info("server: wait ", _io_threads.size(), " io threads...");

	for (auto&t : _io_threads) {
		t.join();
	}

	logger_info("server: io_threads stoped.");
	_is_runned_flag.store(false);
    logger_info("server: stoped.");
  }

  void disconnect_all() {
    for (auto &kv : _clients) {
      if (kv.second->state != ClientState::DISCONNECTED) {
        kv.second->end_session();
        while (kv.second->queue_size() != 0) {
          logger_info("server: wait stop of #", kv.first);
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
      }
      kv.second->close();
    }
    _clients.clear();
  }

  void start() {
    logger_info("server: start server on ", _params.port, "...");
	
	reset_ping_timer();

    ip::tcp::endpoint ep(ip::tcp::v4(), _params.port);
    _acc = acceptor_ptr{new ip::tcp::acceptor(_service, ep)};
    socket_ptr sock(new ip::tcp::socket(_service));
    start_accept(sock);

	_service.poll_one();

    logger_info("server: start ", _params.io_threads, " io threads...");

	_io_threads.resize(_params.io_threads);
	for (size_t i = 0; i < _params.io_threads; ++i) {
		_io_threads[i]=std::move(std::thread(std::bind(&Server::Private::handle_clients_thread, this)));
	}
	
	_is_runned_flag.store(true);
	logger_info("server: OK");
	
  }

  void handle_clients_thread() {
	  _service.run();
  }

  void start_accept(socket_ptr sock) {
    _acc->async_accept(*sock, std::bind(&Server::Private::handle_accept, this, sock, _1));
  }

  void handle_accept(socket_ptr sock, const boost::system::error_code &err) {
    if (err) {
      THROW_EXCEPTION_SS("dariadb::server: error on accept - " << err.message());
    }

    logger_info("server: accept connection.");

    auto cur_id = _next_client_id.load();
    _next_client_id++;
    ClientIO_ptr new_client{new ClientIO(cur_id, sock, this, this->_storage)};
	
    _clients_locker.lock();
    _clients.insert(std::make_pair(new_client->id(), new_client));
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
      THROW_EXCEPTION_SS("server: client_connect - client #" << id << " not found");
    }
    auto client = fres_it->second;
    _connections_accepted += 1;
    logger_info("server: hello from {", client->host, "}, #", client->id());
    client->state = ClientState::WORK;
  }

  void client_disconnect(int id) override {
    _clients_locker.lock();
    auto fres = _clients.find(id);
    if (fres == _clients.end()) {
      THROW_EXCEPTION_SS("server: client_disconnect - client #" << id << " not found");
    }
    fres->second->sock->close();
    _clients.erase(fres);
    _connections_accepted -= 1;
    logger_info("server: clients count  ", _clients.size(), " accepted:",
                _connections_accepted.load());
    _clients_locker.unlock();
  }

  void write_begin() override { _writes_in_progress++; }
  void write_end() override { _writes_in_progress--; }

  void reset_ping_timer() {
    try {
      _ping_timer.expires_from_now(boost::posix_time::millisec(PING_TIMER_INTERVAL));
      _ping_timer.async_wait(std::bind(&Server::Private::on_check_ping, this));
    } catch (std::exception &ex) {
      THROW_EXCEPTION_SS("server: reset_ping_timer - " << ex.what());
    }
  }

  void on_check_ping() {
    if (_clients.empty() || _in_stop_logic) {
      return;
    }
    std::list<int> to_remove;
    _clients_locker.lock();
    for (auto &kv : _clients) {
      if (kv.second->state == ClientState::CONNECT) {
        continue;
      }

      bool is_stoped = kv.second->state == ClientState::DISCONNECTED;
      if (kv.second->pings_missed > MAX_MISSED_PINGS || is_stoped) {
        kv.second->close();
        to_remove.push_back(kv.first);
      } else {
		  logger_info("server: ping #",kv.first);
        kv.second->ping();
      }
    }
    _clients_locker.unlock();

    for (auto &id : to_remove) {
      logger("server: remove #", id);
      client_disconnect(id);
      _clients.erase(id);
    }

    reset_ping_timer();
  }

  io_service _service;
  acceptor_ptr _acc;

  std::atomic_int _next_client_id;
  std::atomic_size_t _connections_accepted;
  
  Server::Param _params;

  std::vector<std::thread> _io_threads;

  std::atomic_bool _stop_flag;
  std::atomic_bool _is_runned_flag;

  std::unordered_map<int, ClientIO_ptr> _clients;
  utils::Locker _clients_locker;

  deadline_timer _ping_timer;

  storage::IMeasStorage *_storage;
  std::atomic_int _writes_in_progress;

  bool _in_stop_logic; // TODO union with _stop_flag
};

Server::Server(const Param &p) : _Impl(new Server::Private(p)) {}

Server::~Server() {}

bool Server::is_runned() {
  return _Impl->is_runned();
}

size_t Server::connections_accepted() const {
  return _Impl->connections_accepted();
}

void Server::start() {
	_Impl->start();
}

void Server::stop() {
  _Impl->stop();
}

void Server::set_storage(storage::IMeasStorage *storage) {
  _Impl->set_storage(storage);
}
