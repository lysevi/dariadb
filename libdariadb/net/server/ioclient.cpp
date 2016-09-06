#include "ioclient.h"
#include "../../meas.h"
#include "../../timeutil.h"
#include "../../utils/exception.h"
#include <json/json.hpp>

using namespace std::placeholders;
using namespace boost::asio;

using namespace dariadb;
using namespace dariadb::net;

ClientIO::ClientIO(int _id, NetData_Pool*pool, socket_ptr &_sock, IClientManager *_srv,
                   storage::IMeasStorage *_storage):AsyncConnection(pool) {
  pings_missed = 0;
  state = ClientState::CONNECT;
  set_id(_id);
  sock = _sock;
  srv = _srv;
  storage = _storage;
  this->start(sock);
}

ClientIO::~ClientIO() { this->full_stop(); }

void ClientIO::end_session() {
  logger("server: #", this->id(), " send disconnect signal.");
  this->state = ClientState::DISCONNECTED;

  if (sock->is_open()) {
    this->sock->cancel();
	auto nd = this->get_pool()->construct();
	nd->append(DataKinds::DISCONNECT);
    this->send(nd);
  }
}

void ClientIO::close() {
  state = ClientState::DISCONNECTED;
  mark_stoped();
  if (this->sock->is_open()) {
    this->sock->cancel();

    full_stop();

    this->sock->close();
  }
  logger_info("server: client #", this->id(), " stoped.");
}

void ClientIO::ping() {
  pings_missed++;
  auto nd = this->get_pool()->construct();
  nd->append(DataKinds::PING);
  this->send(nd);
}

void ClientIO::onNetworkError(const boost::system::error_code &err) {
  if (state != ClientState::DISCONNECTED) {
    // TODO check this moment.
    logger_info("server: client #", this->id(), " network error - ",
                err.message());
    logger_info("server: client #", this->id(), " stoping...");
    return;
  }
  this->close();
}

void ClientIO::onDataRecv(const NetData_ptr &d, bool &cancel) {
  logger("server: #", this->id(), " dataRecv ", d->size, " bytes.");

  if (d->data[0] == (uint8_t)DataKinds::HELLO) {
    std::string msg((char *)&d->data[1], (char *)(d->data + d->size - 1));
    host = msg;
    this->srv->client_connect(this->id());

	auto nd = get_pool()->construct();
    nd->size = sizeof(DataKinds::HELLO) + sizeof(uint32_t);

    nd->data[0] = (uint8_t)DataKinds::HELLO;
    auto idptr = (uint32_t *)(&nd->data[1]);
    *idptr = id();

    this->send(nd);
    return;
  }

  if (d->data[0] == (uint8_t)DataKinds::PONG) {
    pings_missed--;
    logger("server: #", this->id(), " pings_missed: ", pings_missed.load());
    return;
  }

  if (d->data[0] == (uint8_t)DataKinds::DISCONNECT) {
    logger("server: #", this->id(), " disconnection request.");
    cancel = true;
    this->end_session();
    // this->srv->client_disconnect(this->id);
    return;
  }
}

//
// void ClientIO::readNextQuery() {
//  async_read_until(*sock.get(), query_buff, '\n',
//                   std::bind(&ClientIO::onReadQuery, this, _1, _2));
//}
//
// void ClientIO::readHello() {
//  async_read_until(*sock.get(), query_buff, '\n',
//                   std::bind(&ClientIO::onHello, this, _1, _2));
//}
//
// void ClientIO::onHello(const boost::system::error_code &err, size_t
// read_bytes) {
//  if (err) {
//    THROW_EXCEPTION_SS("server: ClienIO::onHello " << err.message());
//  }
//
//  std::istream query_iss(&this->query_buff);
//  std::string msg;
//  std::getline(query_iss, msg);
//
//  if (read_bytes < HELLO_PREFIX.size()) {
//    logger_fatal("server: bad hello size.");
//  } else {
//    std::istringstream hello_iss(msg);
//    std::string readed_str;
//    hello_iss >> readed_str;
//    if (readed_str != HELLO_PREFIX) {
//      THROW_EXCEPTION_SS("server: bad hello prefix " << readed_str);
//    }
//    hello_iss >> readed_str;
//    host = readed_str;
//    this->srv->client_connect(this->id);
//    async_write(*this->sock.get(), buffer(OK_ANSWER + " 0\n"),
//                std::bind(&ClientIO::onOkSended, this, _1, _2));
//  }
//  this->readNextQuery();
//}
//
// void ClientIO::onReadQuery(const boost::system::error_code &err, size_t
// read_bytes) {
//  logger("server: #", this->id, " onReadQuery...");
//  if (this->state == ClientState::DISCONNECTED) {
//    logger_info("server: #", this->id, " onRead in disconnected.");
//    return;
//  }
//  if (err) {
//    THROW_EXCEPTION_SS("server: ClienIO::onRead " << err.message());
//  }
//
//  std::istream iss(&this->query_buff);
//  std::string msg;
//  std::getline(iss, msg);
//  logger("server: #", this->id, " clientio::onReadQuery - {", msg, "}
//  readed_bytes: ",
//         read_bytes);
//
//  if (msg.size() > WRITE_QUERY.size() &&
//      msg.substr(0, WRITE_QUERY.size()) == WRITE_QUERY) {
//    size_t to_write_count = stoi(msg.substr(WRITE_QUERY.size() + 1,
//    msg.size()));
//    logger("server: write query ", to_write_count);
//
//    size_t buffer_size = to_write_count * sizeof(Meas);
//    this->in_values_buffer.resize(to_write_count);
//
//    this->sock->async_read_some(
//        buffer(this->in_values_buffer, buffer_size),
//        std::bind(&ClientIO::onRecvValues, this, to_write_count, _1, _2));
//    return;
//  }
//
//  if (msg.size() > READ_INTERVAL_QUERY.size() &&
//      msg.substr(0, READ_INTERVAL_QUERY.size()) == READ_INTERVAL_QUERY) {
//    auto query_str = msg.substr(READ_INTERVAL_QUERY.size() + 1, msg.size());
//
//    auto id_end_pos = query_str.find_first_of(' ');//position between query_id
//    and json values
//    auto query_id = stoi(query_str.substr(0, id_end_pos));
//
//	query_str = query_str.substr(id_end_pos, query_str.size());
//
//	dariadb::storage::QueryInterval qi({}, 0, 0, 0);
//    qi.from_string(query_str);
//
//    logger_info("server: #", this->id, " query ", query_id, " read interval
//    [",
//                timeutil::to_string(qi.from), ',', timeutil::to_string(qi.to),
//                "] ",
//                qi.ids.size(), " values");
//
//	auto values = this->storage->readInterval(qi);
//    auto size_to_send=values.size();
//    logger_info("server: #", this->id, " query ", query_id, " readed ",
//    size_to_send, " values");
//
//	std::stringstream ss;
//    ss << OK_ANSWER << ' ' << query_id << ' ' << size_to_send;
//	nlohmann::json js;
//	auto js_array = nlohmann::json::array();
//	for (auto v : values) {
//		nlohmann::json meas_js;
//		meas_js["id"] = v.id;
//		meas_js["flag"] = v.flag;
//		meas_js["src"] = v.src;
//		meas_js["value"] = v.value;
//		js_array.push_back(meas_js);
//	}
//	js["result"] = js_array;
//	ss << ' ' << js.dump()<<'\n';
//	auto str = ss.str();
//	async_write(*sock.get(), buffer(str),
//		std::bind(&ClientIO::onReadIntervalAnswerSended, this, _1, _2));
//
//  }
//
//  if (msg == DISCONNECT_PREFIX) {
//    this->disconnect();
//    return;
//  }
//
//  if (msg == PONG_ANSWER) {
//    pings_missed--;
//    logger("server: #", this->id, " pings_missed: ", pings_missed.load());
//  }
//  this->readNextQuery();
//}
//
// void ClientIO::disconnect() {
//  logger("server: #", this->id, " send disconnect signal.");
//  this->state = ClientState::DISCONNECTED;
//  async_write(*sock.get(), buffer(DISCONNECT_ANSWER + "\n"),
//              std::bind(&ClientIO::onDisconnectSended, this, _1, _2));
//}
//
// void ClientIO::onDisconnectSended(const boost::system::error_code &, size_t)
// {
//  logger("server: #", this->id, " onDisconnectSended.");
//  this->sock->close();
//  this->srv->client_disconnect(this->id);
//}
//
// void ClientIO::ping() {
//  pings_missed++;
//  async_write(*sock.get(), buffer(PING_QUERY + "\n"),
//              std::bind(&ClientIO::onPingSended, this, _1, _2));
//}
// void ClientIO::onPingSended(const boost::system::error_code &err, size_t) {
//  if (err) {
//    THROW_EXCEPTION_SS("server::onPingSended - " << err.message());
//  }
//  logger("server: #", this->id, " ping.");
//}
//
// void ClientIO::onOkSended(const boost::system::error_code &err, size_t) {
//  if (err) {
//    THROW_EXCEPTION_SS("server::onOkSended - " << err.message());
//  }
//}
//
// void ClientIO::onRecvValues(size_t values_count, const
// boost::system::error_code &err,
//                            size_t read_bytes) {
//  if (err) {
//    THROW_EXCEPTION_SS("server::readValues - " << err.message());
//  } else {
//    logger_info("clientio: recv bytes ", read_bytes);
//  }
//
//  // TODO use batch loading
//  if (this->storage != nullptr) {
//    logger("server: #", this->id, " write ", in_values_buffer.size(), "
//    values.");
//    this->srv->write_begin();
//    this->storage->append(this->in_values_buffer.begin(),
//    this->in_values_buffer.end());
//    this->srv->write_end();
//  } else {
//    logger_info("clientio: storage no set.");
//  }
//  readNextQuery();
//}
//
// void ClientIO::onReadIntervalAnswerSended(const boost::system::error_code
// &err, size_t read_bytes) {
//	if (err) {
//		THROW_EXCEPTION_SS("server::onReadIntervalAnswerSended - " <<
//err.message());
//	}
//	logger("server: #", this->id, " send ", in_values_buffer.size(), "
//values.");
//	readNextQuery();
//}
//
