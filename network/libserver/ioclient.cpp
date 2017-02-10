#include <cassert>
#include <libdariadb/meas.h>
#include <libdariadb/timeutil.h>
#include <libdariadb/utils/exception.h>
#include <libserver/ioclient.h>

using namespace std::placeholders;
using namespace boost::asio;

using namespace dariadb;
using namespace dariadb::net;

struct SubscribeCallback : public IReadCallback {
  utils::async::Locker _locker;
  IOClient *_parent;
  QueryNumber _query_num;

  SubscribeCallback(IOClient *parent, QueryNumber query_num) {
    _parent = parent;
    _query_num = query_num;
  }
  ~SubscribeCallback() {}
  void apply(const Meas &m) override { send_buffer(m); }
  void is_end() override {}
  void send_buffer(const Meas &m) {
    auto nd = _parent->env->nd_pool->construct(DATA_KINDS::APPEND);
    nd->size = sizeof(QueryAppend_header);

    auto hdr = reinterpret_cast<QueryAppend_header *>(&nd->data);
    hdr->id = _query_num;
    size_t space_left = 0;
    QueryAppend_header::make_query(hdr, &m, size_t(1), 0, &space_left);

    auto size_to_write = NetData::MAX_MESSAGE_SIZE - MARKER_SIZE - space_left;
    nd->size = static_cast<NetData::MessageSize>(size_to_write);

    _parent->_async_connection->send(nd);
  }
};

IOClient::ClientDataReader::ClientDataReader(IOClient *parent,
                                             QueryNumber query_num) {
  _parent = parent;
  pos = 0;
  _query_num = query_num;
  assert(_query_num != 0);
}

void IOClient::ClientDataReader::apply(const Meas &m) {
  std::lock_guard<utils::async::Locker> lg(_locker);
  if (pos == BUFFER_LENGTH) {
    send_buffer();
    pos = 0;
  }
  _buffer[pos++] = m;
}

void IOClient::ClientDataReader::is_end() {
  IReadCallback::is_end();
  send_buffer();

  auto nd = _parent->env->nd_pool->construct(DATA_KINDS::APPEND);
  nd->size = sizeof(QueryAppend_header);
  auto hdr = reinterpret_cast<QueryAppend_header *>(&nd->data);
  hdr->id = _query_num;
  hdr->count = 0;
  logger("server: #", _parent->_async_connection->id(), " end of #", hdr->id);
  _parent->_async_connection->send(nd);
  this->_parent->readerRemove(this->_query_num);
}

void IOClient::ClientDataReader::send_buffer() {
  if (pos == 0) {
    return;
  }

  size_t writed = 0;

  while (writed != pos) {
    auto nd = _parent->env->nd_pool->construct(DATA_KINDS::APPEND);
    nd->size = sizeof(QueryAppend_header);

    auto hdr = reinterpret_cast<QueryAppend_header *>(&nd->data);
    hdr->id = _query_num;
    size_t space_left = 0;
    QueryAppend_header::make_query(hdr, _buffer.data(), pos, writed,
                                   &space_left);

    logger_info("server: pack count: ", hdr->count);

    auto size_to_write = NetData::MAX_MESSAGE_SIZE - MARKER_SIZE - space_left;
    nd->size = static_cast<NetData::MessageSize>(size_to_write);
    writed += hdr->count;

    logger("server: #", _parent->_async_connection->id(),
           " send to client result of #", hdr->id, " count ", hdr->count);
    _parent->_async_connection->send(nd);
  }
}

IOClient::ClientDataReader::~ClientDataReader() {}

IOClient::IOClient(int _id, socket_ptr &_sock, IOClient::Environment *_env) {
  subscribe_reader = nullptr;
  pings_missed = 0;
  state = CLIENT_STATE::CONNECT;
  sock = _sock;
  env = _env;
  _last_query_time = dariadb::timeutil::current_time();

  AsyncConnection::onDataRecvHandler on_d =
      [this](const NetData_ptr &d, bool &cancel, bool &dont_free_memory) {
        onDataRecv(d, cancel, dont_free_memory);
      };
  AsyncConnection::onNetworkErrorHandler on_n =
      [this](const boost::system::error_code &err) { onNetworkError(err); };
  _async_connection = std::shared_ptr<AsyncConnection>{
      new AsyncConnection(_env->nd_pool, on_d, on_n)};
  _async_connection->set_id(_id);
}

IOClient::~IOClient() {
  if (_async_connection != nullptr) {
    _async_connection->full_stop();
  }

  for (auto kv : _readers) {
    logger_info("server: stop reader #", kv.first);
    kv.second.first->cancel();
    kv.second.first->wait();
    this->readerRemove(kv.first);
  }
}

void IOClient::end_session() {
  if (this->state == CLIENT_STATE::DISCONNETION_START) {
    return;
  }
  logger_info("server: #", _async_connection->id(), " send disconnect signal.");
  this->state = CLIENT_STATE::DISCONNETION_START;

  if (sock->is_open()) {
    auto nd =
        this->_async_connection->get_pool()->construct(DATA_KINDS::DISCONNECT);
    this->_async_connection->send(nd);
  }
}

void IOClient::close() {
  if (state != CLIENT_STATE::DISCONNECTED) {
    state = CLIENT_STATE::DISCONNECTED;
    _async_connection->mark_stoped();
    if (this->sock->is_open()) {
      _async_connection->full_stop();

      this->sock->close();
    }
    logger_info("server: client #", this->_async_connection->id(), " stoped.");
    _async_connection = nullptr;
  }
}

void IOClient::ping() {
  auto delta_time = (dariadb::timeutil::current_time() - _last_query_time);
  if (delta_time < PING_TIMER_INTERVAL) {
    return;
  }
  pings_missed++;
  auto nd = this->_async_connection->get_pool()->construct(DATA_KINDS::PING);
  this->_async_connection->send(nd);
}

void IOClient::onNetworkError(const boost::system::error_code &err) {
  if ((state != CLIENT_STATE::DISCONNECTED) &&
      (state != CLIENT_STATE::DISCONNETION_START)) {
    logger_info("server: client #", this->_async_connection->id(),
                " network error - ", err.message());
    logger_info("server: client #", this->_async_connection->id(),
                " stoping...");
    this->close();
  }
}

void IOClient::onDataRecv(const NetData_ptr &d, bool &cancel,
                          bool &dont_free_memory) {
  _last_query_time = dariadb::timeutil::current_time();
  // logger("server: #", this->id(), " dataRecv ", d->size, " bytes.");
  auto qh = reinterpret_cast<Query_header *>(d->data);

  DATA_KINDS kind = (DATA_KINDS)qh->kind;
  switch (kind) {
  case DATA_KINDS::APPEND: {
    if (this->env->srv->server_begin_stopping()) {
      logger_info("server: #", this->_async_connection->id(),
                  " refuse append query. server in stop.");
      return;
    }
    auto hdr = reinterpret_cast<QueryAppend_header *>(&d->data);
    auto count = hdr->count;
    logger_info("server: #", this->_async_connection->id(), " recv #", hdr->id,
                " write ", count);
    this->env->srv->write_begin();

    this->append(d);

    break;
  }
  case DATA_KINDS::PONG: {
    pings_missed--;
    logger_info("server: #", this->_async_connection->id(),
                " pings_missed: ", pings_missed.load());
    break;
  }
  case DATA_KINDS::STAT: {
	  auto st_hdr = reinterpret_cast<QueryStat_header *>(&d->data);
	  auto result=this->env->storage->stat(st_hdr->id, st_hdr->from, st_hdr->to);
	  auto nd = this->env->nd_pool->construct(DATA_KINDS::STAT);

	  auto p_header = reinterpret_cast<QueryStatResult_header *>(nd->data);
	  nd->size = sizeof(QueryStatResult_header);
	  p_header->id = st_hdr->id;
	  p_header->result = result;

	  _async_connection->send(nd);
	  break;
  }
  case DATA_KINDS::DISCONNECT: {
    logger_info("server: #", this->_async_connection->id(),
                " disconnection request.");
    cancel = true;
    this->end_session();
    env->srv->client_disconnect(this->_async_connection->id());
    break;
  }
  case DATA_KINDS::READ_INTERVAL: {
    if (this->env->srv->server_begin_stopping()) {
      logger_info("server: #", this->_async_connection->id(),
                  " refuse read_interval query. server in stop.");
      return;
    }
    auto query_hdr = reinterpret_cast<QueryInterval_header *>(&d->data);

    sendOk(query_hdr->id);
    this->readInterval(d);

    break;
  }
  case DATA_KINDS::READ_TIMEPOINT: {
    if (this->env->srv->server_begin_stopping()) {
      logger_info("server: #", this->_async_connection->id(),
                  " refuse read_timepoint query. server in stop.");
      return;
    }
    auto query_hdr = reinterpret_cast<QueryTimePoint_header *>(&d->data);

    sendOk(query_hdr->id);
    this->readTimePoint(d);
    break;
  }
  case DATA_KINDS::CURRENT_VALUE: {
    if (this->env->srv->server_begin_stopping()) {
      logger_info("server: #", this->_async_connection->id(),
                  " refuse current_value query. server in stop.");
      return;
    }
    auto query_hdr = reinterpret_cast<QueryCurrentValue_header *>(&d->data);
    sendOk(query_hdr->id);
    this->currentValue(d);
    break;
  }
  case DATA_KINDS::SUBSCRIBE: {
    if (this->env->srv->server_begin_stopping()) {
      logger_info("server: #", this->_async_connection->id(),
                  " refuse subscribe query. server in stop.");
      return;
    }
    auto query_hdr = reinterpret_cast<QuerSubscribe_header *>(&d->data);

    sendOk(query_hdr->id);
    subscribe(d);

    break;
  }
  case DATA_KINDS::HELLO: {
    if (this->env->srv->server_begin_stopping()) {
      logger_info("server: #", this->_async_connection->id(),
                  " refuse connection query. server in stop.");
      return;
    }
    QueryHello_header *qhh = reinterpret_cast<QueryHello_header *>(d->data);
    if (qhh->version != PROTOCOL_VERSION) {
      logger("server: #", _async_connection->id(),
             " wrong protocol version: exp=", PROTOCOL_VERSION,
             ", rec=", qhh->version);
      sendError(0, ERRORS::WRONG_PROTOCOL_VERSION);
      this->state = CLIENT_STATE::DISCONNECTED;
      return;
    }
    auto host_ptr = ((char *)(&qhh->host_size) + sizeof(qhh->host_size));

    std::string msg(host_ptr, host_ptr + qhh->host_size);
    host = msg;
    env->srv->client_connect(this->_async_connection->id());

    auto nd = _async_connection->get_pool()->construct(DATA_KINDS::HELLO);
    nd->size += sizeof(uint32_t);
    auto idptr = (uint32_t *)(&nd->data[1]);
    *idptr = _async_connection->id();

    this->_async_connection->send(nd);
    break;
  }
  case DATA_KINDS::REPACK: {
    if (this->env->srv->server_begin_stopping()) {
      logger_info("server: #", this->_async_connection->id(),
                  " refuse repack query. server in stop.");
      return;
    }
    logger_info("server: #", this->_async_connection->id(),
                " query to storage repack.");
    if (this->env->storage->strategy() == STRATEGY::WAL) {
      auto wals = this->env->storage->description().wal_count;
      logger_info("server: #", this->_async_connection->id(), " drop ", wals,
                  " wals to pages.");
      this->env->storage->drop_part_wals(wals);
      this->env->storage->flush();
    }
    //auto query_hdr = reinterpret_cast<QuerRepack_header *>(&d->data);
    this->env->storage->repack();
    break;
  }
  default:
    logger_fatal("server: unknow query kind - ", (uint8_t)kind);
    break;
  }
}

void IOClient::sendOk(QueryNumber query_num) {
  auto ok_nd = env->nd_pool->construct(DATA_KINDS::OK);
  auto qh = reinterpret_cast<QueryOk_header *>(ok_nd->data);
  qh->id = query_num;
  assert(qh->id != 0);
  ok_nd->size = sizeof(QueryOk_header);
  _async_connection->send(ok_nd);
}

void IOClient::sendError(QueryNumber query_num, const ERRORS &err) {
  auto err_nd = env->nd_pool->construct(DATA_KINDS::OK);
  auto qh = reinterpret_cast<QueryError_header *>(err_nd->data);
  qh->id = query_num;
  qh->error_code = (uint16_t)err;
  err_nd->size = sizeof(QueryError_header);
  _async_connection->send(err_nd);
}

void IOClient::append(const NetData_ptr &d) {
  auto hdr = reinterpret_cast<QueryAppend_header *>(d->data);
  auto count = hdr->count;
  logger_info("server: #", this->_async_connection->id(), " begin writing ",
              count);
  MeasArray ma = hdr->read_measarray();

  auto ar = env->storage->append(ma.begin(), ma.end());
  this->env->srv->write_end();
  if (ar.ignored != size_t(0)) {
    logger_info("server: write error - ", ar.error_message);
    sendError(hdr->id, ERRORS::APPEND_ERROR);
  } else {
    sendOk(hdr->id);
  }
  logger_info("server: #", this->_async_connection->id(), " writed ", ar.writed,
              " ignored ", ar.ignored);
}

void IOClient::readInterval(const NetData_ptr &d) {
  auto query_hdr = reinterpret_cast<QueryInterval_header *>(d->data);

  auto from_str = timeutil::to_string(query_hdr->from);
  auto to_str = timeutil::to_string(query_hdr->from);

  logger_info("server: #", this->_async_connection->id(),
              " read interval point #", query_hdr->id, " id(",
              query_hdr->ids_count, ") [", from_str, ',', to_str, "]");
  auto ids_ptr =
      (Id *)((char *)(&query_hdr->ids_count) + sizeof(query_hdr->ids_count));
  IdArray all_ids{ids_ptr, ids_ptr + query_hdr->ids_count};

  auto query_num = query_hdr->id;

  if (query_hdr->from >= query_hdr->to) {
    sendError(query_num, ERRORS::WRONG_QUERY_PARAM_FROM_GE_TO);
  } else {

    auto qi = new QueryInterval{all_ids, query_hdr->flag,
                                         query_hdr->from, query_hdr->to};

    auto cdr = new ClientDataReader(this, query_num);
    this->readerAdd(ReaderCallback_ptr(cdr), qi);
    env->storage->foreach (*qi, cdr);
  }
}

void IOClient::readTimePoint(const NetData_ptr &d) {
  auto query_hdr = reinterpret_cast<QueryTimePoint_header *>(d->data);

  auto tp_str = timeutil::to_string(query_hdr->tp);

  logger_info("server: #", this->_async_connection->id(), " read time point  #",
              query_hdr->id, " id(", query_hdr->ids_count, ") [", tp_str, "]");
  auto ids_ptr =
      (Id *)((char *)(&query_hdr->ids_count) + sizeof(query_hdr->ids_count));
  IdArray all_ids{ids_ptr, ids_ptr + query_hdr->ids_count};

  auto query_num = query_hdr->id;
  auto qi =
      new QueryTimePoint{all_ids, query_hdr->flag, query_hdr->tp};

  auto cdr = new ClientDataReader(this, query_num);
  readerAdd(ReaderCallback_ptr(cdr), qi);
  env->storage->foreach (*qi, cdr);
}

void IOClient::currentValue(const NetData_ptr &d) {
  auto query_hdr = reinterpret_cast<QueryCurrentValue_header *>(d->data);

  logger_info("server: #", this->_async_connection->id(), " current values  #",
              query_hdr->id, " id(", query_hdr->ids_count, ")");
  auto ids_ptr =
      (Id *)((char *)(&query_hdr->ids_count) + sizeof(query_hdr->ids_count));
  IdArray all_ids{ids_ptr, ids_ptr + query_hdr->ids_count};
  auto flag = query_hdr->flag;
  auto query_num = query_hdr->id;

  auto result = env->storage->currentValue(all_ids, flag);
  auto cdr = new ClientDataReader(this, query_num);
  readerAdd(ReaderCallback_ptr(cdr), nullptr);
  for (auto &v : result) {
    cdr->apply(v.second);
  }
  cdr->is_end();
}

void IOClient::subscribe(const NetData_ptr &d) {
  auto query_hdr = reinterpret_cast<QuerSubscribe_header *>(d->data);

  logger_info("server: #", this->_async_connection->id(),
              " subscribe to values  #", query_hdr->id, " id(",
              query_hdr->ids_count, ")");
  auto ids_ptr =
      (Id *)((char *)(&query_hdr->ids_count) + sizeof(query_hdr->ids_count));
  IdArray all_ids{ids_ptr, ids_ptr + query_hdr->ids_count};
  auto flag = query_hdr->flag;
  auto query_num = query_hdr->id;

  if (subscribe_reader == nullptr) {
    subscribe_reader = std::shared_ptr<IReadCallback>(
        new SubscribeCallback(this, query_num));
  }
  env->storage->subscribe(all_ids, flag, subscribe_reader);
}

void IOClient::readerAdd(const ReaderCallback_ptr &cdr, void *data) {
  std::lock_guard<std::mutex> lg(_readers_lock);
  ClientDataReader *cdr_raw = dynamic_cast<ClientDataReader *>(cdr.get());
  ENSURE(cdr_raw != nullptr);
  this->_readers.insert(
      std::make_pair(cdr_raw->_query_num, std::make_pair(cdr, data)));
}

void IOClient::readerRemove(QueryNumber number) {
  std::lock_guard<std::mutex> lg(_readers_lock);
  auto fres = this->_readers.find(number);
  if (fres == this->_readers.end()) {
    THROW_EXCEPTION("engien: readerRemove logic error");
  } else {
    auto ptr = fres->second;
    this->_readers.erase(fres);
    if (ptr.second != nullptr) {
      delete ptr.second;
    }
  }
}