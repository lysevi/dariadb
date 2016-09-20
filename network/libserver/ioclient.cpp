#include "ioclient.h"
#include <libdariadb/meas.h>
#include <libdariadb/timeutil.h>
#include <libdariadb/utils/exception.h>
#include <json/json.hpp>
#include <cassert>

using namespace std::placeholders;
using namespace boost::asio;

using namespace dariadb;
using namespace dariadb::net;

struct SubscribeCallback : public storage::IReaderClb {
	utils::Locker _locker;
	IOClient *_parent;
	QueryNumber _query_num;

	SubscribeCallback(IOClient *parent, QueryNumber query_num) {
		_parent = parent;
		_query_num = query_num;
	}
	~SubscribeCallback() {
	}
	void call(const Meas &m) override {
		send_buffer(m);
	}
	void is_end() override {
	}
	void send_buffer(const Meas &m) {
		auto nd = _parent->env->nd_pool->construct(DataKinds::APPEND);
		nd->size = sizeof(QueryAppend_header);

		auto hdr = reinterpret_cast<QueryAppend_header *>(&nd->data);
		hdr->id = _query_num;
		hdr->count = 1;

		auto size_to_write = hdr->count * sizeof(Meas);

		auto meas_ptr = (Meas *)((char *)(&hdr->count) + sizeof(hdr->count));
		nd->size += static_cast<NetData::MessageSize>(size_to_write);

		*meas_ptr = m;
		_parent->_async_connection->send(nd);
	}
};

IOClient::ClientDataReader::ClientDataReader(IOClient *parent, QueryNumber query_num) {
  _parent = parent;
  pos = 0;
  _query_num = query_num;
  assert(_query_num!=0);
}

void IOClient::ClientDataReader::call(const Meas &m) {
  std::lock_guard<utils::Locker> lg(_locker);
  if (pos == BUFFER_LENGTH) {
    send_buffer();
    pos = 0;
  }
  _buffer[pos++] = m;
}

void IOClient::ClientDataReader::is_end() {
  send_buffer();

  auto nd = _parent->env->nd_pool->construct(DataKinds::APPEND);
  nd->size = sizeof(QueryAppend_header);
  auto hdr = reinterpret_cast<QueryAppend_header *>(&nd->data);
  hdr->id = _query_num;
  hdr->count = 0;
  _parent->_async_connection->send(nd);
}

void IOClient::ClientDataReader::send_buffer() {
  if (pos == 0) {
    return;
  }
  auto nd = _parent->env->nd_pool->construct(DataKinds::APPEND);
  nd->size = sizeof(QueryAppend_header);

  auto hdr = reinterpret_cast<QueryAppend_header *>(&nd->data);
  hdr->id = _query_num;
  hdr->count = static_cast<uint32_t>(pos);

  auto size_to_write = hdr->count * sizeof(Meas);

  auto meas_ptr = (Meas *)((char *)(&hdr->count) + sizeof(hdr->count));
  nd->size += static_cast<NetData::MessageSize>(size_to_write);

  auto it = _buffer.begin();
  size_t i = 0;
  for (; it != _buffer.end() && i < hdr->count; ++it, ++i) {
    *meas_ptr = *it;
    ++meas_ptr;
  }
  logger("server: #", _parent->_async_connection->id(), " send to client result of #", hdr->id," count ", hdr->count);
  _parent->_async_connection->send(nd);
}

IOClient::ClientDataReader::~ClientDataReader() {}

IOClient::IOClient(int _id, socket_ptr &_sock, IOClient::Environment *_env) {
  subscribe_reader = nullptr;
  pings_missed = 0;
  state = ClientState::CONNECT;
  sock = _sock;
  env = _env;
  
  AsyncConnection::onDataRecvHandler on_d = [this](const NetData_ptr &d, bool &cancel, bool &dont_free_memory) {
	  onDataRecv(d, cancel, dont_free_memory);
  };
  AsyncConnection::onNetworkErrorHandler on_n = [this](const boost::system::error_code &err) {
	  onNetworkError(err);
  };
  _async_connection = std::shared_ptr<AsyncConnection>{ new AsyncConnection(_env->nd_pool, on_d, on_n) };
  _async_connection->set_id(_id);

}

IOClient::~IOClient() {
	_async_connection->full_stop();
}

void IOClient::end_session() {
  logger_info("server: #", _async_connection->id(), " send disconnect signal.");
  this->state = ClientState::DISCONNECTED;

  if (sock->is_open()) {
    auto nd = this->_async_connection->get_pool()->construct(DataKinds::DISCONNECT);
    this->_async_connection->send(nd);
  }
}

void IOClient::close() {
  state = ClientState::DISCONNECTED;
  _async_connection->mark_stoped();
  if (this->sock->is_open()) {
	  _async_connection->full_stop();

    this->sock->close();
  }
  logger_info("server: client #", this->_async_connection->id(), " stoped.");
}

void IOClient::ping() {
  pings_missed++;
  auto nd = this->_async_connection->get_pool()->construct(DataKinds::PING);
  this->_async_connection->send(nd);
}

void IOClient::onNetworkError(const boost::system::error_code &err) {
  if (state != ClientState::DISCONNECTED) {
    // TODO check this moment.
    logger_info("server: client #", this->_async_connection->id(), " network error - ", err.message());
    logger_info("server: client #", this->_async_connection->id(), " stoping...");
    return;
  }
  this->close();
}

void IOClient::onDataRecv(const NetData_ptr &d, bool &cancel,
                          bool &dont_free_memory) {
  // logger("server: #", this->id(), " dataRecv ", d->size, " bytes.");
  auto qh = reinterpret_cast<Query_header *>(d->data);

  DataKinds kind = (DataKinds)qh->kind;
  switch (kind) {
  case DataKinds::APPEND: {
    auto hdr = reinterpret_cast<QueryAppend_header *>(&d->data);
    auto count = hdr->count;
    logger_info("server: #", this->_async_connection->id(), " recv #", hdr->id,
                " write ", count);
    this->env->srv->write_begin();
    dont_free_memory = true;

    env->service->post(
        env->io_meases_strand->wrap(std::bind(&IOClient::append, this, d)));

    break;
  }
  case DataKinds::PONG: {
    pings_missed--;
    logger_info("server: #", this->_async_connection->id(), " pings_missed: ",
                pings_missed.load());
    break;
  }
  case DataKinds::DISCONNECT: {
    logger_info("server: #", this->_async_connection->id(),
                " disconnection request.");
    cancel = true;
    this->end_session();
    // this->srv->client_disconnect(this->id);
    break;
  }
  case DataKinds::READ_INTERVAL: {
    auto query_hdr = reinterpret_cast<QueryInterval_header *>(&d->data);
    dont_free_memory = true;
    sendOk(query_hdr->id);
    env->service->post(env->io_meases_strand->wrap(
        std::bind(&IOClient::readInterval, this, d)));
    break;
  }
  case DataKinds::READ_TIMEPOINT: {
    auto query_hdr = reinterpret_cast<QueryTimePoint_header *>(&d->data);
    dont_free_memory = true;
    sendOk(query_hdr->id);
    env->service->post(env->io_meases_strand->wrap(
        std::bind(&IOClient::readTimePoint, this, d)));
    break;
  }
  case DataKinds::CURRENT_VALUE: {
    auto query_hdr = reinterpret_cast<QueryCurrentValue_header *>(&d->data);
    dont_free_memory = true;
    sendOk(query_hdr->id);
    env->service->post(env->io_meases_strand->wrap(
        std::bind(&IOClient::currentValue, this, d)));
    break;
  }
  case DataKinds::SUBSCRIBE: {
    auto query_hdr = reinterpret_cast<QuerSubscribe_header *>(&d->data);
    dont_free_memory = true;
    sendOk(query_hdr->id);
    env->service->post(
        env->io_meases_strand->wrap(std::bind(&IOClient::subscribe, this, d)));
    break;
  }
  case DataKinds::HELLO: {
    QueryHello_header *qhh = reinterpret_cast<QueryHello_header *>(d->data);
    if (qhh->version != PROTOCOL_VERSION) {
      logger("server: #", _async_connection->id(),
             " wrong protocol version: exp=", PROTOCOL_VERSION, ", rec=",
             qhh->version);
      sendError(0, ERRORS::WRONG_PROTOCOL_VERSION);
      this->state = ClientState::DISCONNECTED;
      return;
    }
    auto host_ptr = ((char *)(&qhh->host_size) + sizeof(qhh->host_size));

    std::string msg(host_ptr, host_ptr + qhh->host_size);
    host = msg;
    env->srv->client_connect(this->_async_connection->id());

    auto nd = _async_connection->get_pool()->construct(DataKinds::HELLO);
    nd->size += sizeof(uint32_t);
    auto idptr = (uint32_t *)(&nd->data[1]);
    *idptr = _async_connection->id();

    this->_async_connection->send(nd);
    break;
  }
  default:
    logger_fatal("server: unknow query kind - ", (uint8_t)kind);
    break;
  }
}

void IOClient::sendOk(QueryNumber query_num) {
  auto ok_nd = env->nd_pool->construct(DataKinds::OK);
  auto qh = reinterpret_cast<QueryOk_header *>(ok_nd->data);
  qh->id = query_num;
  assert(qh->id!=0);
  ok_nd->size = sizeof(QueryOk_header);
  _async_connection->send(ok_nd);
}

void IOClient::sendError(QueryNumber query_num, const ERRORS &err) {
  auto err_nd = env->nd_pool->construct(DataKinds::OK);
  auto qh = reinterpret_cast<QueryError_header *>(err_nd->data);
  qh->id = query_num;
  qh->error_code = (uint16_t)err;
  err_nd->size = sizeof(QueryError_header);
  _async_connection->send(err_nd);
}

void IOClient::append(const NetData_ptr &d) {
  auto hdr = reinterpret_cast<QueryAppend_header *>(d->data);
  auto count = hdr->count;
  logger_info("server: #", this->_async_connection->id(), " begin writing ", count);
  MeasArray ma = hdr->read_measarray();

  auto ar = env->storage->append(ma.begin(), ma.end());
  this->env->srv->write_end();
  sendOk(hdr->id);
  this->env->nd_pool->free(d);
  logger_info("server: #", this->_async_connection->id(), " writed ", ar.writed, " ignored ", ar.ignored);
}

void IOClient::readInterval(const NetData_ptr &d) {
  auto query_hdr = reinterpret_cast<QueryInterval_header *>(d->data);

  auto from_str = timeutil::to_string(query_hdr->from);
  auto to_str = timeutil::to_string(query_hdr->from);

  logger_info("server: #", this->_async_connection->id(), " read interval point #", query_hdr->id, " id(",
              query_hdr->ids_count, ") [", from_str, ',', to_str, "]");
  auto ids_ptr = (Id *)((char *)(&query_hdr->ids_count) + sizeof(query_hdr->ids_count));
  IdArray all_ids{ids_ptr, ids_ptr + query_hdr->ids_count};

  auto query_num = query_hdr->id;
  storage::QueryInterval qi{all_ids, query_hdr->flag, query_hdr->source, query_hdr->from,
                            query_hdr->to};

  env->nd_pool->free(d);

  if (query_hdr->from >= query_hdr->to) {
    sendError(query_num, ERRORS::WRONG_QUERY_PARAM_FROM_GE_TO);
  } else {
    // TODO use shared ptr
    ClientDataReader *cdr = new ClientDataReader(this, query_num);
    env->storage->foreach (qi, cdr);
    delete cdr;
  }
}

void IOClient::readTimePoint(const NetData_ptr &d) {
  auto query_hdr = reinterpret_cast<QueryTimePoint_header *>(d->data);

  auto tp_str = timeutil::to_string(query_hdr->tp);

  logger_info("server: #", this->_async_connection->id(), " read time point  #", query_hdr->id, " id(",
              query_hdr->ids_count, ") [", tp_str, "]");
  auto ids_ptr = (Id *)((char *)(&query_hdr->ids_count) + sizeof(query_hdr->ids_count));
  IdArray all_ids{ids_ptr, ids_ptr + query_hdr->ids_count};

  auto query_num = query_hdr->id;
  storage::QueryTimePoint qi{all_ids, query_hdr->flag, query_hdr->tp};

  env->nd_pool->free(d);

  // TODO use shared ptr
  ClientDataReader *cdr = new ClientDataReader(this, query_num);
  env->storage->foreach (qi, cdr);
  delete cdr;
}

void  IOClient::currentValue(const NetData_ptr &d) {
	auto query_hdr = reinterpret_cast<QueryCurrentValue_header *>(d->data);

	logger_info("server: #", this->_async_connection->id(), " current values  #", query_hdr->id, " id(",
		query_hdr->ids_count, ")");
	auto ids_ptr = (Id *)((char *)(&query_hdr->ids_count) + sizeof(query_hdr->ids_count));
	IdArray all_ids{ ids_ptr, ids_ptr + query_hdr->ids_count };
	auto flag = query_hdr->flag;
	auto query_num = query_hdr->id;
	env->nd_pool->free(d);

	// TODO use shared ptr
	auto result = env->storage->currentValue(all_ids, flag);
	ClientDataReader *cdr = new ClientDataReader(this, query_num);
	for (auto&v : result) {
		cdr->call(v.second);
	}
	cdr->is_end();
	delete cdr;
	// auto result = env->storage->readTimePoint(qi);
}

void  IOClient::subscribe(const NetData_ptr &d) {
	auto query_hdr = reinterpret_cast<QuerSubscribe_header *>(d->data);

	logger_info("server: #", this->_async_connection->id(), " subscribe to values  #", query_hdr->id, " id(",
		query_hdr->ids_count, ")");
	auto ids_ptr = (Id *)((char *)(&query_hdr->ids_count) + sizeof(query_hdr->ids_count));
	IdArray all_ids{ ids_ptr, ids_ptr + query_hdr->ids_count };
	auto flag = query_hdr->flag;
	auto query_num = query_hdr->id;
	env->nd_pool->free(d);

    if(subscribe_reader==nullptr){
        subscribe_reader = std::shared_ptr<storage::IReaderClb>(new SubscribeCallback(this, query_num));
    }
	env->storage->subscribe(all_ids, flag, subscribe_reader);
}
