#include <libdariadb/utils/exception.h>
#include <libdariadb/utils/logger.h>
#include <libserver/iclientmanager.h>
#include <libserver/ioclient.h>
#include <libserver/server.h>
#include <boost/asio.hpp>
#include <atomic>
#include <common/net_common.h>
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

const int INFO_TIMER_INTERVAL = 10000;
const int MAX_MISSED_PINGS = 100;

class Server::Private : public IClientManager {
public:
  Private(const Server::Param &p)
      : _signals(_service, SIGINT, SIGTERM, SIGABRT), 
        _params(p), _is_runned_flag(false), _ping_timer(_service), _info_timer(_service) {

    _in_stop_logic = false;
    _next_client_id = 1;
    _connections_accepted.store(0);
    _writes_in_progress.store(0);

    _env.srv = this;
    _env.nd_pool = &_net_data_pool;
    _env.service = &_service;

	_signals.async_wait(std::bind(&Server::Private::signal_handler, this, _1, _2));
  }

  ~Private() { if (_is_runned_flag && ! _in_stop_logic) { stop(); } }

  void set_storage(storage::Engine *storage) {
    logger("server: set storage.");
    _env.storage = storage;
	log_server_info_internal();
  }

  void stop() {
    _in_stop_logic = true;
    logger_info("server: *** [stopping] ***");
	logger_info("server: stop ping timer...");
	_ping_timer.cancel();
	logger_info("server: stop info timer...");
	_info_timer.cancel();
    while (_writes_in_progress.load() != 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(300));
      logger_info("server: writes in progress ", _writes_in_progress.load());
    }

    disconnect_all();

    logger_info("server: stop asio service.");
    _service.stop();
	while (!_service.stopped()) {}
    logger_info("server: wait ", _io_threads.size(), " io threads...");

    for (auto &t : _io_threads) {
		if (t.joinable()) {
			t.join();
		}
    }
    logger_info("server: io_threads stoped.");

    logger_info("server: stoping storage engine...");
    if(this->_env.storage!=nullptr){ //in some tests storage not exists
		auto cp = this->_env.storage;
		this->_env.storage = nullptr;
        cp->stop();
    }

	_in_stop_logic = false;
    _is_runned_flag.store(false);
    logger_info("server: stoped.");
  }

  void disconnect_all() {
    for (auto &kv : _clients) {
      if (kv.second->state != CLIENT_STATE::DISCONNECTED) {
        kv.second->end_session();
      }
    }

	for (auto &kv : _clients) {
		if (kv.second->state != CLIENT_STATE::DISCONNECTED) {
			while (kv.second->_async_connection->queue_size() != 0) {
				logger_info("server: wait stop of #", kv.first);
				std::this_thread::sleep_for(std::chrono::milliseconds(50));
			}
			kv.second->close();
		}
	}
    _clients.clear();
  }

  void start() {
    logger_info("server: start server on ", _params.port, "...");

    reset_ping_timer();
	reset_info_timer();

    ip::tcp::endpoint ep(ip::tcp::v4(), _params.port);
    _acc = acceptor_ptr{new ip::tcp::acceptor(_service, ep)};
    socket_ptr sock(new ip::tcp::socket(_service));
    start_accept(sock);

    _service.poll_one();

    logger_info("server: start ", _params.io_threads, " io threads...");

    _io_threads.resize(_params.io_threads);
    for (size_t i = 0; i < _params.io_threads; ++i) {
        auto t=std::thread(std::bind(&Server::Private::handle_clients_thread, this));
      _io_threads[i] = std::move(t);
    }

    _is_runned_flag.store(true);
    logger_info("server: ready.");
  }

  void signal_handler(const boost::system::error_code &error, int signal_number) {
    if (!error) {
      switch (signal_number) {
      case SIGINT:
        logger_info("server: *** [signal handler - SIGINT] ***");
		this->stop();
        break;
      case SIGTERM:
        logger_info("server: *** [signal handler - SIGTERM] ***");
		this->stop();
        break;
      case SIGABRT:
        logger_info("server: *** [signal handler - SIGABRT] ***");
		this->stop();
        break;
      default:
        logger_info("server: signal handler - unknow");
        break;
      };
    } else {
      logger_fatal("server: signal handler error - ", error.message());
    }
  }
  void handle_clients_thread() { asio_run();  }

  void asio_run() {
	  _service.run();
  }
  void start_accept(socket_ptr sock) {
    _acc->async_accept(*sock, std::bind(&Server::Private::handle_accept, this, sock, _1));
  }

  void handle_accept(socket_ptr sock, const boost::system::error_code &err) {
    if (err) {
      THROW_EXCEPTION("dariadb::server: error on accept - " , err.message());
    }
	if (this->_in_stop_logic) {
		logger_info("server: in stop logic. connection refused.");
	}
	else {
		logger_info("server: accept connection.");

		auto cur_id = _next_client_id.load();
		_next_client_id++;

		ClientIO_ptr new_client{ new IOClient(cur_id, sock, &_env) };

		_clients_locker.lock();
		_clients.emplace(std::make_pair(new_client->_async_connection->id(), new_client));
		_clients_locker.unlock();

		new_client->start();
		socket_ptr new_sock(new ip::tcp::socket(_service));
		start_accept(new_sock);
	}
  }

  size_t connections_accepted() const { return _connections_accepted.load(); }

  bool is_runned() { return _is_runned_flag.load(); }

  void client_connect(int id) override {
    std::lock_guard<utils::Locker> lg(_clients_locker);
    auto fres_it = this->_clients.find(id);
    if (fres_it == this->_clients.end()) {
      THROW_EXCEPTION("server: client_connect - client #" , id , " not found");
    }
    auto client = fres_it->second;
    _connections_accepted += 1;
    logger_info("server: hello from {", client->host, "}, #", client->_async_connection->id());
    client->state = CLIENT_STATE::WORK;
  }

  void client_disconnect(int id) override {
    std::lock_guard<utils::Locker> lg(_clients_locker);
    auto fres = _clients.find(id);
    if (fres == _clients.end()) {
        // may be already removed.
        return;
    }
    //fres->second->sock->close();
    _clients.erase(fres);
    _connections_accepted -= 1;
    logger_info("server: clients count  ", _clients.size(), " accepted:",
                _connections_accepted.load());
  }

  void write_begin() override { _writes_in_progress++; }
  void write_end() override { _writes_in_progress--; }
  bool server_begin_stopping()const override { return _in_stop_logic; }

  void reset_ping_timer() {
    try {
      _ping_timer.expires_from_now(boost::posix_time::millisec(PING_TIMER_INTERVAL));
      _ping_timer.async_wait(std::bind(&Server::Private::ping_all, this));
    } catch (std::exception &ex) {
      THROW_EXCEPTION("server: reset_ping_timer - " , ex.what());
    }
  }

  void ping_all() {
    if (_clients.empty() || _in_stop_logic) {
      return;
    }
    std::list<int> to_remove;
    _clients_locker.lock();
    for (auto &kv : _clients) {
      if (kv.second->state == CLIENT_STATE::CONNECT) {
        continue;
      }

      bool is_stoped = kv.second->state == CLIENT_STATE::DISCONNECTED;
      if (kv.second->pings_missed > MAX_MISSED_PINGS || is_stoped) {
        kv.second->close();
        to_remove.push_back(kv.first);
      } else {
        logger_info("server: ping #", kv.first);
        kv.second->ping();
      }
    }
    _clients_locker.unlock();


    for (auto &id : to_remove) {
      logger_info("server: remove #", id);
      client_disconnect(id);
    }
    reset_ping_timer();
  }

  void reset_info_timer() {
	  try {
		  _info_timer.expires_from_now(boost::posix_time::millisec(INFO_TIMER_INTERVAL));
		  _info_timer.async_wait(std::bind(&Server::Private::log_server_info, this));
	  }
	  catch (std::exception &ex) {
          THROW_EXCEPTION("server: reset_ping_timer - " , ex.what());
	  }
  }

  void log_server_info_internal() {
	  if (_env.storage == nullptr) {//for tests.
		  return;
	  }
      auto queue_sizes = _env.storage->description();
	  std::stringstream stor_ss;

      stor_ss << "(p:" << queue_sizes.pages_count << " a:" << queue_sizes.aofs_count
          << " T:" << queue_sizes.active_works;
      if (_env.storage->strategy() == dariadb::storage::STRATEGY::MEMORY) {
          stor_ss << " am:" << queue_sizes.memstorage.allocator_capacity
              << " a:" << queue_sizes.memstorage.allocated;
      }
      stor_ss << ")";

	  logger_info("server: stat ", stor_ss.str());
  }
  void log_server_info() {
	log_server_info_internal();
	reset_info_timer();
  }

  io_service _service;
  boost::asio::signal_set _signals;
  acceptor_ptr _acc;

  std::atomic_int _next_client_id;
  std::atomic_size_t _connections_accepted;

  Server::Param _params;

  std::vector<std::thread> _io_threads;

  std::atomic_bool _is_runned_flag;

  std::unordered_map<int, ClientIO_ptr> _clients;
  utils::Locker _clients_locker;

  deadline_timer _ping_timer;
  deadline_timer _info_timer;

  IOClient::Environment _env;
  std::atomic_int _writes_in_progress;

  bool _in_stop_logic;
  NetData_Pool _net_data_pool;
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

void Server::set_storage(storage::Engine *storage) {
  _Impl->set_storage(storage);
}

void Server::asio_run() {
	_Impl->asio_run();
}
