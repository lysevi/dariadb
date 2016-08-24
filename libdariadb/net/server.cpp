#include "server.h"
#include "../utils/exception.h"
#include "../utils/logger.h"
#include "net_common.h"
#include <atomic>
#include <boost/asio.hpp>
#include <functional>
#include <istream>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <list>

using namespace std::placeholders;
using namespace boost::asio;

using namespace dariadb;
using namespace dariadb::net;

typedef boost::shared_ptr<ip::tcp::socket> socket_ptr;
typedef boost::shared_ptr<ip::tcp::acceptor> acceptor_ptr;

const int PING_TIMER_INTERVAL = 1000;
const int MAX_MISSED_PINGS = 100;

class Server::Private {
public:
  struct ClientIO {
    int id;
    socket_ptr sock;
    streambuf buff;
    std::string host;

    ClientState state;
    Server::Private *srv;

    std::atomic_int pings_missed;
    ClientIO(int _id, socket_ptr _sock, Server::Private *_srv) {
      pings_missed = 0;
      state = ClientState::CONNECT;
      id = _id;
      sock = _sock;
      srv = _srv;
    }

    void readNext() {
      async_read_until(*sock.get(), buff, '\n',
                       std::bind(&ClientIO::onRead, this, _1, _2));
    }

    void readHello() {
      async_read_until(*sock.get(), buff, '\n',
                       std::bind(&ClientIO::onHello, this, _1, _2));
    }

    void onHello(const boost::system::error_code &err, size_t read_bytes) {
      if (err) {
        THROW_EXCEPTION_SS("server: ClienIO::onHello " << err.message());
      }

      std::istream iss(&this->buff);
      std::string msg;
      std::getline(iss, msg);

      if (read_bytes < HELLO_PREFIX.size()) {
        logger_fatal("server: bad hello size.");
      } else {
        std::istringstream iss(msg);
        std::string readed_str;
        iss >> readed_str;
        if (readed_str != HELLO_PREFIX) {
          THROW_EXCEPTION_SS("server: bad hello prefix " << readed_str);
        }
        iss >> readed_str;
        host = readed_str;
        sock->write_some(buffer(OK_ANSWER + "\n"));
        this->srv->client_connect(this);
      }
      this->readNext();
    }

    void onRead(const boost::system::error_code &err, size_t read_bytes) {
      logger("server: #", this->id, " onRead...");
      if (this->state == ClientState::DISCONNECTED) {
        logger_info("server: #", this->id, " onRead in disconnected.");
        return;
      }
      if (err) {
        THROW_EXCEPTION_SS("server: ClienIO::onRead " << err.message());
      }

      std::istream iss(&this->buff);
      std::string msg;
      std::getline(iss, msg);
      logger("server: #", this->id, " clientio::onRead - {", msg,
             "} readed_bytes: ", read_bytes);

      if (msg == DISCONNECT_PREFIX) {
        this->disconnect();
        return;
      }

      if (msg == PONG_ANSWER) {
        pings_missed--;
        logger("server: #", this->id, " pings_missed: ", pings_missed.load());
      }
      this->readNext();
    }

    void disconnect() {
      logger("server: #", this->id, " send disconnect signal.");
      this->state = ClientState::DISCONNECTED;
      async_write(*sock.get(), buffer(DISCONNECT_ANSWER + "\n"),
                  std::bind(&ClientIO::onDisconnectSended, this, _1, _2));
    }
    void onDisconnectSended(const boost::system::error_code &err,
                            size_t read_bytes) {
      logger("server: #", this->id, " onDisconnectSended.");
      this->sock->close();
      this->srv->client_disconnect(this);
    }

    void ping() {
      pings_missed++;
      async_write(*sock.get(), buffer(PING_QUERY + "\n"),
                  std::bind(&ClientIO::onPingSended, this, _1, _2));
    }
    void onPingSended(const boost::system::error_code &err, size_t read_bytes) {
      if (err) {
        THROW_EXCEPTION_SS("server::onPingSended - " << err.message());
      }
      logger("server: #", this->id, " ping.");
    }
  };

  typedef std::shared_ptr<ClientIO> ClientIO_ptr;

  Private(const Server::Param &p)
      : _params(p), _stop_flag(false), _is_runned_flag(false),
        _ping_timer(_service) {
    _next_id = 0;
    _connections_accepted.store(0);
    _thread_handler = std::thread(&Server::Private::server_thread, this);
    reset_ping_timer();
  }

  ~Private() { stop(); }

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
    ClientIO_ptr new_client{new ClientIO(cur_id, sock, this)};
    new_client->readHello();

    _clients_locker.lock();
    _clients.insert(std::make_pair(new_client->id, new_client));
    _clients_locker.unlock();

    socket_ptr new_sock(new ip::tcp::socket(_service));
    start_accept(new_sock);
  }

  size_t connections_accepted() const { return _connections_accepted.load(); }

  bool is_runned() { return _is_runned_flag.load(); }

  void client_connect(ClientIO *client) {
    _connections_accepted += 1;
    logger_info("server: hello from {", client->host, "}, #", client->id);
    client->state = ClientState::WORK;
  }

  void client_disconnect(ClientIO *client) {
    _clients_locker.lock();
    auto fres = _clients.find(client->id);
    if (fres == _clients.end()) {
      THROW_EXCEPTION_SS("server: client_disconnect - client #"
                         << client->id << " not found");
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
          kv.second->state=ClientState::DISCONNECTED;
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
};

Server::Server(const Param &p) : _Impl(new Server::Private(p)) {}

Server::~Server() {}

bool Server::is_runned() { return _Impl->is_runned(); }

size_t Server::connections_accepted() const {
  return _Impl->connections_accepted();
}

void Server::stop() { _Impl->stop(); }
