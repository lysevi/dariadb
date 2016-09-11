#include "ioclient.h"
#include "../../meas.h"
#include "../../timeutil.h"
#include "../../utils/exception.h"
#include <json/json.hpp>

using namespace std::placeholders;
using namespace boost::asio;

using namespace dariadb;
using namespace dariadb::net;

IOClient::ClientDataReader::ClientDataReader(IOClient *parent, QueryNumber query_num) {
  _parent = parent;
  pos = 0;
  _query_num = query_num;
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

  auto nd = _parent->env->nd_pool->construct(DataKinds::WRITE);
  nd->size = sizeof(QueryWrite_header);
  auto hdr = reinterpret_cast<QueryWrite_header *>(&nd->data);
  hdr->id = _query_num;
  hdr->count = 0;
  _parent->send(nd);
}

void IOClient::ClientDataReader::send_buffer() {
  if (pos == 0) {
    return;
  }
  auto nd = _parent->env->nd_pool->construct(DataKinds::WRITE);
  nd->size = sizeof(QueryWrite_header);

  auto hdr = reinterpret_cast<QueryWrite_header *>(&nd->data);
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
  _parent->send(nd);
}

IOClient::ClientDataReader::~ClientDataReader() {}

IOClient::IOClient(int _id, socket_ptr &_sock, IOClient::Environment *_env)
    : AsyncConnection(_env->nd_pool) {
  pings_missed = 0;
  state = ClientState::CONNECT;
  set_id(_id);
  sock = _sock;
  env = _env;
  this->start(sock);
}

IOClient::~IOClient() {
  this->full_stop();
}

void IOClient::end_session() {
  logger_info("server: #", this->id(), " send disconnect signal.");
  this->state = ClientState::DISCONNECTED;

  if (sock->is_open()) {
    auto nd = this->get_pool()->construct(DataKinds::DISCONNECT);
    this->send(nd);
  }
}

void IOClient::close() {
  state = ClientState::DISCONNECTED;
  mark_stoped();
  if (this->sock->is_open()) {
    full_stop();

    this->sock->close();
  }
  logger_info("server: client #", this->id(), " stoped.");
}

void IOClient::ping() {
  pings_missed++;
  auto nd = this->get_pool()->construct(DataKinds::PING);
  this->send(nd);
}

void IOClient::onNetworkError(const boost::system::error_code &err) {
  if (state != ClientState::DISCONNECTED) {
    // TODO check this moment.
    logger_info("server: client #", this->id(), " network error - ", err.message());
    logger_info("server: client #", this->id(), " stoping...");
    return;
  }
  this->close();
}

void IOClient::onDataRecv(const NetData_ptr &d, bool &cancel, bool &dont_free_memory) {
  logger("server: #", this->id(), " dataRecv ", d->size, " bytes.");
  auto qh = reinterpret_cast<Query_header *>(d->data);

  if (qh->kind == (uint8_t)DataKinds::WRITE) {
    auto hdr = reinterpret_cast<QueryWrite_header *>(&d->data);
    auto count = hdr->count;
    logger_info("server: #", this->id(), " recv #", hdr->id, " write ", count);
    this->env->srv->write_begin();
    dont_free_memory = true;
    env->service->post(env->write_meases_strand->wrap(
        std::bind(&IOClient::writeMeasurementsCall, this, d)));
    sendOk(hdr->id);
    return;
  }

  if (qh->kind == (uint8_t)DataKinds::PONG) {
    pings_missed--;
    logger_info("server: #", this->id(), " pings_missed: ", pings_missed.load());
    return;
  }

  if (qh->kind == (uint8_t)DataKinds::DISCONNECT) {
    logger_info("server: #", this->id(), " disconnection request.");
    cancel = true;
    this->end_session();
    // this->srv->client_disconnect(this->id);
    return;
  }

  if (qh->kind == (uint8_t)DataKinds::READ_INTERVAL) {
    auto query_hdr = reinterpret_cast<QueryInterval_header *>(&d->data);
    dont_free_memory = true;
    env->service->post(
        env->write_meases_strand->wrap(std::bind(&IOClient::readInterval, this, d)));
    sendOk(query_hdr->id);
    return;
  }

  if (qh->kind == (uint8_t)DataKinds::READ_TIMEPOINT) {
    auto query_hdr = reinterpret_cast<QueryTimePoint_header *>(&d->data);
    dont_free_memory = true;
    env->service->post(
        env->write_meases_strand->wrap(std::bind(&IOClient::readTimePoint, this, d)));
    sendOk(query_hdr->id);
    return;
  }

  if (qh->kind == (uint8_t)DataKinds::HELLO) {
    QueryHello_header *qh = reinterpret_cast<QueryHello_header *>(d->data);
    if (qh->version != PROTOCOL_VERSION) {
      logger("server: #", id(), " wrong protocol version: exp=", PROTOCOL_VERSION,
             ", rec=", qh->version);
      sendError(0, ERRORS::WRONG_PROTOCOL_VERSION);
      this->state = ClientState::DISCONNECTED;
      return;
    }
    auto host_ptr = ((char *)(&qh->host_size) + sizeof(qh->host_size));

    std::string msg(host_ptr, host_ptr + qh->host_size);
    host = msg;
    env->srv->client_connect(this->id());

    auto nd = get_pool()->construct(DataKinds::HELLO);
    nd->size += sizeof(uint32_t);
    auto idptr = (uint32_t *)(&nd->data[1]);
    *idptr = id();

    this->send(nd);
    return;
  }
}

void IOClient::sendOk(QueryNumber query_num) {
  auto ok_nd = env->nd_pool->construct(DataKinds::OK);
  auto qh = reinterpret_cast<QueryOk_header *>(ok_nd->data);
  qh->id = query_num;
  ok_nd->size = sizeof(QueryOk_header);
  send(ok_nd);
}

void IOClient::sendError(QueryNumber query_num, const ERRORS &err) {
  auto err_nd = env->nd_pool->construct(DataKinds::OK);
  auto qh = reinterpret_cast<QueryError_header *>(err_nd->data);
  qh->id = query_num;
  qh->error_code = (uint16_t)err;
  err_nd->size = sizeof(QueryError_header);
  send(err_nd);
}

void IOClient::writeMeasurementsCall(const NetData_ptr &d) {
  auto hdr = reinterpret_cast<QueryWrite_header *>(d->data);
  auto count = hdr->count;
  logger_info("server: #", this->id(), " begin async writing ", count, "...");
  Meas::MeasArray ma = hdr->read_measarray();

  auto ar = env->storage->append(ma.begin(), ma.end());
  this->env->srv->write_end();
  this->env->nd_pool->free(d);
  logger_info("server: #", this->id(), " writed ", ar.writed, " ignored ", ar.ignored);
}

void IOClient::readInterval(const NetData_ptr &d) {
  auto query_hdr = reinterpret_cast<QueryInterval_header *>(d->data);

  auto from_str = timeutil::to_string(query_hdr->from);
  auto to_str = timeutil::to_string(query_hdr->from);

  logger_info("server: #", this->id(), " read interval point #", query_hdr->id, " id(",
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

  logger_info("server: #", this->id(), " read time point  #", query_hdr->id, " id(",
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
  // auto result = env->storage->readInTimePoint(qi);
}
