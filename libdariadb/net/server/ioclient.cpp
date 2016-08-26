#include "ioclient.h"
#include "../../utils/exception.h"

using namespace std::placeholders;
using namespace boost::asio;

using namespace dariadb;
using namespace dariadb::net;

ClientIO::ClientIO(int _id, socket_ptr _sock, IClientManager *_srv) {
  pings_missed = 0;
  state = ClientState::CONNECT;
  id = _id;
  sock = _sock;
  srv = _srv;
}

void ClientIO::readNext() {
  async_read_until(*sock.get(), buff, '\n',
                   std::bind(&ClientIO::onRead, this, _1, _2));
}

void ClientIO::readHello() {
  async_read_until(*sock.get(), buff, '\n',
                   std::bind(&ClientIO::onHello, this, _1, _2));
}

void ClientIO::onHello(const boost::system::error_code &err,
                       size_t read_bytes) {
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
    this->srv->client_connect(this->id);
    async_write(*this->sock.get(), buffer(OK_ANSWER + " 0\n"),
                std::bind(&ClientIO::onOkSended, this, _1, _2));
  }
  this->readNext();
}

void ClientIO::onRead(const boost::system::error_code &err, size_t read_bytes) {
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

void ClientIO::disconnect() {
  logger("server: #", this->id, " send disconnect signal.");
  this->state = ClientState::DISCONNECTED;
  async_write(*sock.get(), buffer(DISCONNECT_ANSWER + "\n"),
              std::bind(&ClientIO::onDisconnectSended, this, _1, _2));
}
void ClientIO::onDisconnectSended(const boost::system::error_code &err,
                                  size_t read_bytes) {
  logger("server: #", this->id, " onDisconnectSended.");
  this->sock->close();
  this->srv->client_disconnect(this->id);
}

void ClientIO::ping() {
  pings_missed++;
  async_write(*sock.get(), buffer(PING_QUERY + "\n"),
              std::bind(&ClientIO::onPingSended, this, _1, _2));
}
void ClientIO::onPingSended(const boost::system::error_code &err,
                            size_t read_bytes) {
  if (err) {
    THROW_EXCEPTION_SS("server::onPingSended - " << err.message());
  }
  logger("server: #", this->id, " ping.");
}

void ClientIO::onOkSended(const boost::system::error_code &err,
                          size_t read_bytes) {
  if (err) {
    THROW_EXCEPTION_SS("server::onOkSended - " << err.message());
  }
}
