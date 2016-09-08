#include "ioclient.h"
#include "../../meas.h"
#include "../../timeutil.h"
#include "../../utils/exception.h"
#include <json/json.hpp>

using namespace std::placeholders;
using namespace boost::asio;

using namespace dariadb;
using namespace dariadb::net;

ClientIO::ClientIO(int _id, socket_ptr &_sock, ClientIO::Environment *_env):AsyncConnection(_env->nd_pool) {
  pings_missed = 0;
  state = ClientState::CONNECT;
  set_id(_id);
  sock = _sock;
  env = _env;
  this->start(sock);
}

ClientIO::~ClientIO() { this->full_stop(); }

void ClientIO::end_session() {
  logger_info("server: #", this->id(), " send disconnect signal.");
  this->state = ClientState::DISCONNECTED;

  if (sock->is_open()) {
	/*boost::system::error_code err;
    this->sock->cancel(err);
	if (err) {
		logger_fatal("server: on clientio::end_session - ", err.message());
	}*/
	auto nd = this->get_pool()->construct(DataKinds::DISCONNECT);
    this->send(nd);
  }
}

void ClientIO::close() {
  state = ClientState::DISCONNECTED;
  mark_stoped();
  if (this->sock->is_open()) {
    full_stop();

    this->sock->close();
  }
  logger_info("server: client #", this->id(), " stoped.");
}

void ClientIO::ping() {
  pings_missed++;
  auto nd = this->get_pool()->construct(DataKinds::PING);
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

void ClientIO::onDataRecv(const NetData_ptr &d, bool &cancel, bool&dont_free_memory) {
  logger("server: #", this->id(), " dataRecv ", d->size, " bytes.");
  auto qh = reinterpret_cast<Query_header*>(d->data);

  if (qh->kind == (uint8_t)DataKinds::WRITE) {
	  auto hdr = reinterpret_cast<QueryWrite_header*>(&d->data);
	  auto count = hdr->count;
	  logger_info("server: #", this->id(), " recv #", hdr->id, " write ", count);
	  this->env->srv->write_begin();
	  dont_free_memory = true;
	  env->service->post(env->write_meases_strand->wrap(std::bind(&ClientIO::writeMeasurementsCall, this, d)));
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
	  auto query_hdr = reinterpret_cast<QueryInterval_header*>(&d->data);
	  dont_free_memory = true;
	  env->service->post(env->read_meases_strand->wrap(std::bind(&ClientIO::readInterval, this, d)));
	  sendOk(query_hdr->id);
	  return;
  }

  if (qh->kind == (uint8_t)DataKinds::HELLO) {
	  std::string msg((char *)&d->data[1], (char *)(d->data + d->size - 1));
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

void ClientIO::sendOk(QueryNumber query_num) {
	auto ok_nd = env->nd_pool->construct(DataKinds::OK);
	auto qh = reinterpret_cast<QueryOk_header*>(ok_nd->data);
	qh->id = query_num;
	ok_nd->size+=sizeof(query_num);
	send(ok_nd);
}

void ClientIO::writeMeasurementsCall(const NetData_ptr&d) {
	auto hdr = reinterpret_cast<QueryWrite_header*>(d->data);
	auto count = hdr->count;
	logger_info("server: #", this->id(), " begin async writing ", count,"...");
	Meas::MeasArray ma = hdr->read_measarray();

	auto ar = env->storage->append(ma.begin(), ma.end());
	this->env->srv->write_end();
	this->env->nd_pool->free(d);
	logger_info("server: #", this->id(), " writed ", ar.writed, " ignored ", ar.ignored);
}

void ClientIO::readInterval(const NetData_ptr&d) {
	auto query_hdr = reinterpret_cast<QueryInterval_header*>(d->data);
	
	auto from_str = timeutil::to_string(query_hdr->from);
	auto to_str = timeutil::to_string(query_hdr->from);

	logger_info("server: #", this->id(), " read query #", query_hdr->id, " id(",query_hdr->ids_count,") [",from_str,',',to_str,"]");
	auto ids_ptr = (Id*)((char*)(&query_hdr->ids_count) + sizeof(query_hdr->ids_count));
	IdArray all_ids{ ids_ptr, ids_ptr + query_hdr->ids_count };

	auto query_num = query_hdr->id;
	storage::QueryInterval qi{ all_ids,query_hdr->flag, query_hdr->source,query_hdr->from,query_hdr->to };
	
	env->nd_pool->free(d);

	auto result = env->storage->readInterval(qi);
	size_t result_size = result.size();
	size_t writed = 0;
	auto source_it = result.begin();
	while (writed != result_size) {
		auto left = (result_size - writed);

		auto cur_msg_space = (NetData::MAX_MESSAGE_SIZE - 1 - sizeof(QueryWrite_header));
		size_t count_to_write = (left * sizeof(Meas))>cur_msg_space ? cur_msg_space / sizeof(Meas) : left;
		logger_info("server: #",id()," pack count ", count_to_write);


		auto size_to_write = count_to_write * sizeof(Meas);
		
		auto nd = env->nd_pool->construct(DataKinds::WRITE);
		nd->size += sizeof(QueryWrite_header);

		auto hdr = reinterpret_cast<QueryWrite_header*>(&nd->data);
		hdr->id = query_num;
		hdr->count = static_cast<uint32_t>(count_to_write);

		auto meas_ptr = (Meas*)((char*)(&hdr->count) + sizeof(hdr->count));
		nd->size += static_cast<NetData::MessageSize>(size_to_write);
		auto it = source_it;
		size_t i = 0;
		for (; it != result.end() && i<count_to_write; ++it, ++i) {
			*meas_ptr = *it;
			++source_it;
			++meas_ptr;
		}
		send(nd);
		writed += count_to_write;
	}

	auto nd = env->nd_pool->construct(DataKinds::WRITE);
	nd->size += sizeof(QueryWrite_header);
	auto hdr = reinterpret_cast<QueryWrite_header*>(&nd->data);
	hdr->id = query_num;
	hdr->count = 0;
	send(nd);

}
