#include <libserver/ioclient.h>
#include <libserver/messages.pb.h>
#include <libdariadb/meas.h>
#include <libdariadb/timeutil.h>
#include <libdariadb/utils/exception.h>
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
//		auto nd = _parent->env->nd_pool->construct(DATA_KINDS::APPEND);
//		nd->size = sizeof(QueryAppend_header);

//		auto hdr = reinterpret_cast<QueryAppend_header *>(&nd->data);
//		hdr->id = _query_num;
//		hdr->count = 1;

//		auto size_to_write = hdr->count * sizeof(Meas);

//		auto meas_ptr = (Meas *)((char *)(&hdr->count) + sizeof(hdr->count));
//		nd->size += static_cast<NetData::MessageSize>(size_to_write);

//		*meas_ptr = m;
//		_parent->_async_connection->send(nd);
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

//  auto nd = _parent->env->nd_pool->construct(DATA_KINDS::APPEND);
//  nd->size = sizeof(QueryAppend_header);
//  auto hdr = reinterpret_cast<QueryAppend_header *>(&nd->data);
//  hdr->id = _query_num;
//  hdr->count = 0;
//  logger("server: #", _parent->_async_connection->id(), " end of #", hdr->id);
//  _parent->_async_connection->send(nd);
}

void IOClient::ClientDataReader::send_buffer() {
  if (pos == 0) {
    return;
  }
//  auto nd = _parent->env->nd_pool->construct(DATA_KINDS::APPEND);
//  nd->size = sizeof(QueryAppend_header);

//  auto hdr = reinterpret_cast<QueryAppend_header *>(&nd->data);
//  hdr->id = _query_num;
//  hdr->count = static_cast<uint32_t>(pos);

//  auto size_to_write = hdr->count * sizeof(Meas);

//  auto meas_ptr = (Meas *)((char *)(&hdr->count) + sizeof(hdr->count));
//  nd->size += static_cast<NetData::MessageSize>(size_to_write);

//  auto it = _buffer.begin();
//  size_t i = 0;
//  for (; it != _buffer.end() && i < hdr->count; ++it, ++i) {
//    *meas_ptr = *it;
//    ++meas_ptr;
//  }
//  logger("server: #", _parent->_async_connection->id(), " send to client result of #", hdr->id," count ", hdr->count);
//  _parent->_async_connection->send(nd);
}

IOClient::ClientDataReader::~ClientDataReader() {}

IOClient::IOClient(int _id, socket_ptr &_sock, IOClient::Environment *_env) {
  subscribe_reader = nullptr;
  pings_missed = 0;
  state = CLIENT_STATE::CONNECT;
  sock = _sock;
  env = _env;
  _last_query_time=dariadb::timeutil::current_time();

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
  this->state = CLIENT_STATE::DISCONNECTED;

  if (sock->is_open()) {
      auto nd = _async_connection->get_pool()->construct(
          dariadb::net::messages::DISCONNECT, _async_connection->id());
      this->_async_connection->send(nd);
  }
}

void IOClient::close() {
    if(state != CLIENT_STATE::DISCONNECTED){
        state = CLIENT_STATE::DISCONNECTED;
        _async_connection->mark_stoped();
        if (this->sock->is_open()) {
            _async_connection->full_stop();

            this->sock->close();
        }
        _async_connection=nullptr;
        logger_info("server: client #", this->_async_connection->id(), " stoped.");
    }
}

void IOClient::ping() {
    auto delta_time=(dariadb::timeutil::current_time()-_last_query_time);
    if(delta_time<PING_TIMER_INTERVAL){
        return;
    }
  pings_missed++;
  auto nd = _async_connection->get_pool()->construct(
      dariadb::net::messages::PING, _async_connection->id());
  this->_async_connection->send(nd);
}

void IOClient::onNetworkError(const boost::system::error_code &err) {
    if (state != CLIENT_STATE::DISCONNECTED) {
      logger_info("server: client #", this->_async_connection->id(), " network error - ", err.message());
      logger_info("server: client #", this->_async_connection->id(), " stoping...");
      this->close();
    }
}

void IOClient::onDataRecv(const NetData_ptr &d, bool &cancel,
                          bool &dont_free_memory) {
  _last_query_time=dariadb::timeutil::current_time();
  // logger("server: #", this->id(), " dataRecv ", d->size, " bytes.");
  dariadb::net::messages::QueryHeader qhdr;
  qhdr.ParseFromArray(d->data, d->size);

dariadb::net::messages::QueryKind kind = qhdr.kind();
  switch (kind) {
  case dariadb::net::messages::QueryKind::APPEND: {
      if (this->env->srv->server_begin_stopping()) {
          logger_info("server: #", this->_async_connection->id(), " refuse append query. server in stop.");
          return;
      }
    dont_free_memory = true;

    env->service->post(
        env->io_meases_strand->wrap(std::bind(&IOClient::append, this, d)));

    break;
  }
  case dariadb::net::messages::QueryKind::PONG: {
    pings_missed--;
    logger_info("server: #", this->_async_connection->id(), " pings_missed: ",
                pings_missed.load());
    break;
  }
  case dariadb::net::messages::QueryKind::DISCONNECT: {
    logger_info("server: #", this->_async_connection->id()," disconnection request.");
    cancel = true;
    this->end_session();
    break;
  }
  case dariadb::net::messages::QueryKind::READ_INTERVAL: {
//	  if (this->env->srv->server_begin_stopping()) {
//		  logger_info("server: #", this->_async_connection->id(), " refuse read_interval query. server in stop.");
//		  return;
//	  }
//    auto query_hdr = reinterpret_cast<QueryInterval_header *>(&d->data);
//    dont_free_memory = true;
//    sendOk(query_hdr->id);
//    env->service->post(env->io_meases_strand->wrap(
//        std::bind(&IOClient::readInterval, this, d)));
    break;
  }
  case dariadb::net::messages::QueryKind::READ_TIMEPOINT: {
//	  if (this->env->srv->server_begin_stopping()) {
//		  logger_info("server: #", this->_async_connection->id(), " refuse read_timepoint query. server in stop.");
//		  return;
//	  }
//    auto query_hdr = reinterpret_cast<QueryTimePoint_header *>(&d->data);
//    dont_free_memory = true;
//    sendOk(query_hdr->id);
//    env->service->post(env->io_meases_strand->wrap(
//        std::bind(&IOClient::readTimePoint, this, d)));
    break;
  }
  case dariadb::net::messages::QueryKind::CURRENT_VALUE: {
//	  if (this->env->srv->server_begin_stopping()) {
//		  logger_info("server: #", this->_async_connection->id(), " refuse current_value query. server in stop.");
//		  return;
//	  }
//    auto query_hdr = reinterpret_cast<QueryCurrentValue_header *>(&d->data);
//    dont_free_memory = true;
//    sendOk(query_hdr->id);
//    env->service->post(env->io_meases_strand->wrap(
//        std::bind(&IOClient::currentValue, this, d)));
    break;
  }
  case dariadb::net::messages::QueryKind::SUBSCRIBE: {
//	  if (this->env->srv->server_begin_stopping()) {
//		  logger_info("server: #", this->_async_connection->id(), " refuse subscribe query. server in stop.");
//		  return;
//	  }
//    auto query_hdr = reinterpret_cast<QuerSubscribe_header *>(&d->data);
//    dont_free_memory = true;
//    sendOk(query_hdr->id);
//    env->service->post(
//        env->io_meases_strand->wrap(std::bind(&IOClient::subscribe, this, d)));
    break;
  }
  case dariadb::net::messages::QueryKind::HELLO: {
	  if (this->env->srv->server_begin_stopping()) {
		  logger_info("server: #", this->_async_connection->id(), " refuse connection query. server in stop.");
		  return;
	  }

      dariadb::net::messages::QueryHello qhello;
      qhello.ParseFromString(qhdr.submessage());

    if (qhello.version() != PROTOCOL_VERSION) {
      logger("server: #", _async_connection->id(),
             " wrong protocol version: exp=", PROTOCOL_VERSION, ", rec=",
             qhello.version());
      sendError(0, ERRORS::WRONG_PROTOCOL_VERSION);
      this->state = CLIENT_STATE::DISCONNECTED;
      return;
    }

    host = qhello.host();
    env->srv->client_connect(this->_async_connection->id());

    auto nd = _async_connection->get_pool()->construct(
        dariadb::net::messages::HELLO, _async_connection->id());
    this->_async_connection->send(nd);
    break;
  }
  default:
    logger_fatal("server: unknow query kind - ", (uint8_t)kind);
    break;
  }
}

void IOClient::sendOk(QueryNumber query_num) {
    auto nd = _async_connection->get_pool()->construct(
        dariadb::net::messages::OK, query_num);
    this->_async_connection->send(nd);
}

void IOClient::sendError(QueryNumber query_num, const ERRORS &err) {
    auto nd = _async_connection->get_pool()->construct();
    nd->size=NetData::MAX_MESSAGE_SIZE-MARKER_SIZE;

    dariadb::net::messages::QueryHeader qhdr;
    qhdr.set_id(query_num);
    qhdr.set_kind(dariadb::net::messages::ERR);

    dariadb::net::messages::QueryError qhm;
    qhm.set_errpr_code((uint16_t)err);
    qhdr.set_submessage(qhm.SerializeAsString());

    if(!qhdr.SerializeToArray(nd->data, nd->size)){
        THROW_EXCEPTION("hello message serialize error");
    }

    nd->size=qhdr.ByteSize();


    this->_async_connection->send(nd);
}

void IOClient::append(const NetData_ptr &d) {
    this->env->srv->write_begin();

    dariadb::net::messages::QueryHeader qhdr;
    qhdr.ParseFromArray(d->data, d->size);
    dariadb::net::messages::QueryAppend* qap=qhdr.MutableExtension(dariadb::net::messages::QueryAppend::qappend);
    auto count=qap->values_size();
    logger_info("server: #", this->_async_connection->id(), " recv #", qhdr.id(), " write ", count);

    auto bg=qap->values().begin();
    auto end=qap->values().end();

    size_t ignored=0;
    size_t writed=0;
    for(auto it=bg;it!=end;++it){
         auto va=*it;
         for(auto m_it=va.data().begin();m_it!=va.data().end();++m_it){
            dariadb::Meas m;
            m.id=va.id();
            m.time=m_it->time();
            m.flag=m_it->flag();
            m.value=m_it->value();

            if(env->storage->append(m).ignored!=0){
                ++ignored;
            }else{
                ++writed;
            }
         }
    }

//  auto hdr = reinterpret_cast<QueryAppend_header *>(d->data);
//  auto count = hdr->count;
//  logger_info("server: #", this->_async_connection->id(), " begin writing ", count);
//  MeasArray ma = hdr->read_measarray();

//  auto ar = env->storage->append(ma.begin(), ma.end());
    this->env->srv->write_end();
    sendOk(qhdr.id());
    this->env->nd_pool->free(d);
    logger_info("server: #", this->_async_connection->id()," writed ", writed, " ignored ", ignored);
}

void IOClient::readInterval(const NetData_ptr &d) {
//  auto query_hdr = reinterpret_cast<QueryInterval_header *>(d->data);

//  auto from_str = timeutil::to_string(query_hdr->from);
//  auto to_str = timeutil::to_string(query_hdr->from);

//  logger_info("server: #", this->_async_connection->id(), " read interval point #", query_hdr->id, " id(",
//              query_hdr->ids_count, ") [", from_str, ',', to_str, "]");
//  auto ids_ptr = (Id *)((char *)(&query_hdr->ids_count) + sizeof(query_hdr->ids_count));
//  IdArray all_ids{ids_ptr, ids_ptr + query_hdr->ids_count};

//  auto query_num = query_hdr->id;
//  storage::QueryInterval qi{all_ids, query_hdr->flag, query_hdr->from, query_hdr->to};

//  env->nd_pool->free(d);

//  if (query_hdr->from >= query_hdr->to) {
//    sendError(query_num, ERRORS::WRONG_QUERY_PARAM_FROM_GE_TO);
//  } else {
//    auto cdr = std::make_unique<ClientDataReader>(this, query_num);
//    env->storage->foreach(qi, cdr.get());
//  }
}

void IOClient::readTimePoint(const NetData_ptr &d) {
//  auto query_hdr = reinterpret_cast<QueryTimePoint_header *>(d->data);

//  auto tp_str = timeutil::to_string(query_hdr->tp);

//  logger_info("server: #", this->_async_connection->id(), " read time point  #", query_hdr->id, " id(",
//              query_hdr->ids_count, ") [", tp_str, "]");
//  auto ids_ptr = (Id *)((char *)(&query_hdr->ids_count) + sizeof(query_hdr->ids_count));
//  IdArray all_ids{ids_ptr, ids_ptr + query_hdr->ids_count};

//  auto query_num = query_hdr->id;
//  storage::QueryTimePoint qi{all_ids, query_hdr->flag, query_hdr->tp};

//  env->nd_pool->free(d);

//  auto cdr =std::make_unique<ClientDataReader>(this, query_num);
//  env->storage->foreach(qi, cdr.get());
}

void  IOClient::currentValue(const NetData_ptr &d) {
//	auto query_hdr = reinterpret_cast<QueryCurrentValue_header *>(d->data);

//	logger_info("server: #", this->_async_connection->id(), " current values  #", query_hdr->id, " id(",
//		query_hdr->ids_count, ")");
//	auto ids_ptr = (Id *)((char *)(&query_hdr->ids_count) + sizeof(query_hdr->ids_count));
//	IdArray all_ids{ ids_ptr, ids_ptr + query_hdr->ids_count };
//	auto flag = query_hdr->flag;
//	auto query_num = query_hdr->id;
//	env->nd_pool->free(d);

//	auto result = env->storage->currentValue(all_ids, flag);
//	auto cdr = std::make_unique<ClientDataReader>(this, query_num);
//	for (auto&v : result) {
//		cdr->call(v.second);
//	}
//	cdr->is_end();
}

void  IOClient::subscribe(const NetData_ptr &d) {
//	auto query_hdr = reinterpret_cast<QuerSubscribe_header *>(d->data);

//	logger_info("server: #", this->_async_connection->id(), " subscribe to values  #", query_hdr->id, " id(",
//		query_hdr->ids_count, ")");
//	auto ids_ptr = (Id *)((char *)(&query_hdr->ids_count) + sizeof(query_hdr->ids_count));
//	IdArray all_ids{ ids_ptr, ids_ptr + query_hdr->ids_count };
//	auto flag = query_hdr->flag;
//	auto query_num = query_hdr->id;
//	env->nd_pool->free(d);

//    if(subscribe_reader==nullptr){
//        subscribe_reader = std::shared_ptr<storage::IReaderClb>(new SubscribeCallback(this, query_num));
//    }
//	env->storage->subscribe(all_ids, flag, subscribe_reader);
}
