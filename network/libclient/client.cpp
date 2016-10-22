#include <libdariadb/utils/exception.h>
#include <libdariadb/utils/locker.h>
#include <libdariadb/utils/logger.h>
#include <common/async_connection.h>
#include <common/net_common.h>
#include <libclient/client.h>

#include <boost/asio.hpp>
#include <functional>
#include <map>
#include <memory>
#include <thread>
#include <string>

using namespace std::placeholders;
using namespace boost::asio;

using namespace dariadb;
using namespace dariadb::net;
using namespace dariadb::net::client;

typedef boost::shared_ptr<ip::tcp::socket> socket_ptr;

class Client::Private{
public:
  Private(const Client::Param &p): _params(p) {
    _query_num = 1;
    _state = CLIENT_STATE::CONNECT;
    _pings_answers = 0;
	AsyncConnection::onDataRecvHandler on_d = [this](const NetData_ptr &d, bool &cancel, bool &dont_free_memory) {
		onDataRecv(d, cancel, dont_free_memory);
	};
	AsyncConnection::onNetworkErrorHandler on_n = [this](const boost::system::error_code &err) {
		onNetworkError(err);
	};
	_async_connection = std::shared_ptr<AsyncConnection>{new AsyncConnection(&_pool, on_d, on_n)};
  }

  ~Private() noexcept(false) {
    try {
      if (_state != CLIENT_STATE::DISCONNECTED && _socket != nullptr) {
        this->disconnect();
      }

      _service.stop();
      _thread_handler.join();
    } catch (std::exception &ex) {
      THROW_EXCEPTION("client: #" , _async_connection->id(), ex.what());
    }
  }

  void connect() {
    logger_info("client: connecting to ", _params.host, ':', _params.port);

    _state = CLIENT_STATE::CONNECT;
    auto t=std::thread{&Client::Private::client_thread, this};
    _thread_handler = std::move(t);

    while (this->_state != CLIENT_STATE::WORK) {
      std::this_thread::sleep_for(std::chrono::milliseconds(300));
    }
  }

  void disconnect() {
    if (_socket->is_open()) {
      auto nd = _pool.construct(DATA_KINDS::DISCONNECT);
      this->_async_connection->send(nd);
    }

    while (this->_state != CLIENT_STATE::DISCONNECTED) {
      logger_info("client: #", _async_connection->id(), " disconnect - wait server answer...");
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
  }

  void client_thread() {
      ip::tcp::resolver resolver(_service);

      ip::tcp::resolver::query query(_params.host, std::to_string(_params.port));
      ip::tcp::resolver::iterator iter = resolver.resolve( query);
      ip::tcp::endpoint ep = *iter;

      auto raw_sock_ptr = new ip::tcp::socket(_service);
      _socket = socket_ptr{raw_sock_ptr};
	  _socket->async_connect(ep, [this](auto ec) {
		  if (ec) {
			  THROW_EXCEPTION("dariadb::client: error on connect - ", ec.message());
		  }
		  this->_async_connection->start(this->_socket);
		  std::lock_guard<utils::Locker> lg(_locker);
		  auto hn = ip::host_name();

		  logger_info("client: send hello ", hn);

		  auto nd = this->_pool.construct();
		  nd->size += static_cast<NetData::MessageSize>(hn.size());
		  nd->size += sizeof(QueryHello_header);

		  QueryHello_header *qh = reinterpret_cast<QueryHello_header *>(nd->data);
		  qh->kind = (uint8_t)DATA_KINDS::HELLO;
		  qh->version = PROTOCOL_VERSION;
		  
		  auto host_ptr = ((char *)(&qh->host_size) + sizeof(qh->host_size));
		  
		  
		  auto total_size = (hn.size() + nd->size);
		  auto sz = total_size >= dariadb::net::NetData::MAX_MESSAGE_SIZE ? (NetData::MAX_MESSAGE_SIZE - nd->size) : hn.size();
		  qh->host_size = (uint32_t)sz;
		  memcpy(host_ptr, hn.data(), sz);
		  this->_async_connection->send(nd);
	  });
      _service.run();
  }

  void onNetworkError(const boost::system::error_code &err) {
    if (this->_state != CLIENT_STATE::DISCONNECTED) {
      THROW_EXCEPTION("client: #" , _async_connection->id() , err.message());
    }
  }

  void onDataRecv(const NetData_ptr &d, bool &cancel, bool &) {
    //    if (this->_state == CLIENT_STATE::WORK) {
    //      logger("client: #", id(), " dataRecv ", d->size, " bytes.");
    //    } else {
    //      logger("client: dataRecv ", d->size, " bytes.");
    //    }

    auto qh = reinterpret_cast<Query_header *>(d->data);

    DATA_KINDS kind = (DATA_KINDS)qh->kind;
    switch (kind) {
    case DATA_KINDS::OK: {
      auto qh_ok = reinterpret_cast<QueryOk_header *>(d->data);
      auto query_num = qh_ok->id;
      logger_info("client: #", _async_connection->id(), " query #", query_num,
                  " accepted.");
      if (this->_state != CLIENT_STATE::WORK) {
        THROW_EXCEPTION("(this->_state != CLIENT_STATE::WORK)", this->_state);
      }

      auto subres_it = this->_query_results.find(query_num);
      if (subres_it != this->_query_results.end()) {
        subres_it->second->is_ok = true;
        if (subres_it->second->kind == DATA_KINDS::SUBSCRIBE) {
          subres_it->second->locker.unlock();
        }
      } else {
        THROW_EXCEPTION("client: query #", qh_ok->id, " not found");
      }
      break;
    }
    case DATA_KINDS::ERR: {
      auto qh_e = reinterpret_cast<QueryError_header *>(d->data);
      auto query_num = qh_e->id;
      ERRORS err = (ERRORS)qh_e->error_code;
      logger_info("client: #", _async_connection->id(), " query #", query_num,
                  " error:", err);
      if (this->state() == CLIENT_STATE::WORK) {
        auto subres = this->_query_results[qh_e->id];
        subres->is_closed = true;
        subres->is_error = true;
        subres->errc = err;
        subres->locker.unlock();
        _query_results.erase(qh_e->id);
      }
      break;
    }
    case DATA_KINDS::APPEND: {
      auto qw = reinterpret_cast<QueryAppend_header *>(d->data);
      logger_info("client: #", _async_connection->id(), " recv ", qw->count,
                  " values to query #", qw->id);
      auto subres = this->_query_results[qw->id];
      assert(subres->is_ok);
      if (qw->count == 0) {
        subres->is_closed = true;
        subres->clbk(subres.get(), Meas::empty());
        subres->locker.unlock();
        _query_results.erase(qw->id);
      } else {
        MeasArray ma = qw->read_measarray();
        for (auto &v : ma) {
          subres->clbk(subres.get(), v);
        }
      }
      break;
    }
    case DATA_KINDS::PING: {
      logger_info("client: #", _async_connection->id(), " ping.");
      auto nd = _pool.construct(DATA_KINDS::PONG);
      this->_async_connection->send(nd);
      _pings_answers++;
      break;
    }
    case DATA_KINDS::DISCONNECT: {
      cancel = true;
      logger_info("client: #", _async_connection->id(), " disconnection.");
      try {
        _state = CLIENT_STATE::DISCONNECTED;
        this->_async_connection->full_stop();
        this->_socket->close();
      } catch (...) {
      }
      logger_info("client: #", _async_connection->id(), " disconnected.");
      break;
    }

    // hello id
    case DATA_KINDS::HELLO: {
      auto qh_hello = reinterpret_cast<QueryHelloFromServer_header *>(d->data);
      auto id = qh_hello->id;
      this->_async_connection->set_id(id);
      this->_state = CLIENT_STATE::WORK;
      logger_info("client: #", id, " ready.");
      break;
    }
    default:
      logger_fatal("client: unknow query kind - ", (int)kind);
      break;
    }
  }

  size_t pings_answers() const { return _pings_answers.load(); }
  CLIENT_STATE state() const { return _state; }

  void append(const MeasArray &ma) {
	  this->_locker.lock();
    logger_info("client: send ", ma.size());
    size_t writed = 0;
	std::list<ReadResult_ptr> results;

    while (writed != ma.size()) {
      auto cur_id = _query_num;
      _query_num += 1;
      auto left = (ma.size() - writed);

	  auto qres = std::make_shared<ReadResult>();
	  qres->id = cur_id;
	  qres->kind = DATA_KINDS::APPEND;
	  results.push_back(qres);
	  this->_query_results[qres->id] = qres;

      auto cur_msg_space = (NetData::MAX_MESSAGE_SIZE - 1 - sizeof(QueryAppend_header));
      size_t count_to_write =
          (left * sizeof(Meas)) > cur_msg_space ? cur_msg_space / sizeof(Meas) : left;
      logger_info("client: pack count: ", count_to_write);

      auto size_to_write = count_to_write * sizeof(Meas);

      auto nd = this->_pool.construct(DATA_KINDS::APPEND);
      nd->size = sizeof(QueryAppend_header);

      auto hdr = reinterpret_cast<QueryAppend_header *>(&nd->data);
      hdr->id = cur_id;
      hdr->count = static_cast<uint32_t>(count_to_write);

      auto meas_ptr = ((char *)(&hdr->count) + sizeof(hdr->count));
      memcpy(meas_ptr, ma.data() + writed, size_to_write);
      nd->size += static_cast<NetData::MessageSize>(size_to_write);

	  _async_connection->send(nd);
      writed += count_to_write;
    }
	this->_locker.unlock();
	for (auto&r : results) {
		while (!r->is_ok && !r->is_error) {
			std::this_thread::yield();
		}
		this->_locker.lock();
		this->_query_results.erase(r->id);
		this->_locker.unlock();
	}
  }

  ReadResult_ptr readInterval(const storage::QueryInterval &qi, ReadResult::callback &clbk) {
    _locker.lock();
    auto cur_id = _query_num;
    _query_num += 1;
    _locker.unlock();

    auto qres = std::make_shared<ReadResult>();
    qres->locker.lock();
    qres->id = cur_id;
	qres->kind = DATA_KINDS::READ_INTERVAL;

    auto nd = this->_pool.construct(DATA_KINDS::READ_INTERVAL);

    auto p_header = reinterpret_cast<QueryInterval_header *>(nd->data);
    nd->size = sizeof(QueryInterval_header);
    p_header->id = cur_id;
    p_header->flag = qi.flag;
    p_header->from = qi.from;
    p_header->to = qi.to;

    auto id_size = sizeof(Id) * qi.ids.size();
    if ((id_size + nd->size) > NetData::MAX_MESSAGE_SIZE) {
      _pool.free(nd);
      THROW_EXCEPTION("client: query to big");
    }
    p_header->ids_count = (uint16_t)(qi.ids.size());
    auto ids_ptr = ((char *)(&p_header->ids_count) + sizeof(p_header->ids_count));
    memcpy(ids_ptr, qi.ids.data(), id_size);
    nd->size += static_cast<NetData::MessageSize>(id_size);

    qres->is_closed = false;
    qres->clbk = clbk;
    this->_query_results[qres->id] = qres;

	_async_connection->send(nd);
    return qres;
  }

  MeasList readInterval(const storage::QueryInterval &qi) {
    MeasList result{};
    auto clbk_lambda = [&result](const ReadResult *parent, const Meas &m) {
      if (!parent->is_closed) {
        result.push_back(m);
      }
    };
    ReadResult::callback clbk = clbk_lambda;
    auto qres = readInterval(qi, clbk);
    qres->wait();
    return result;
  }

  ReadResult_ptr readTimePoint(const storage::QueryTimePoint &qi, ReadResult::callback &clbk) {
    _locker.lock();
    auto cur_id = _query_num;
    _query_num += 1;
    _locker.unlock();

    auto qres = std::make_shared<ReadResult>();
    qres->locker.lock();
    qres->id = cur_id;
	qres->kind = DATA_KINDS::READ_TIMEPOINT;

    auto nd = this->_pool.construct(DATA_KINDS::READ_TIMEPOINT);

    auto p_header = reinterpret_cast<QueryTimePoint_header *>(nd->data);
    nd->size = sizeof(QueryTimePoint_header);
    p_header->id = cur_id;
    p_header->flag = qi.flag;
    p_header->tp = qi.time_point;

    auto id_size = sizeof(Id) * qi.ids.size();
    if ((id_size + nd->size) > NetData::MAX_MESSAGE_SIZE) {
      _pool.free(nd);
      THROW_EXCEPTION("client: query to big");
    }
    p_header->ids_count = (uint16_t)(qi.ids.size());
    auto ids_ptr = ((char *)(&p_header->ids_count) + sizeof(p_header->ids_count));
    memcpy(ids_ptr, qi.ids.data(), id_size);
    nd->size += static_cast<NetData::MessageSize>(id_size);

    qres->is_closed = false;
    qres->clbk = clbk;
    this->_query_results[qres->id] = qres;

	_async_connection->send(nd);
    return qres;
  }

  Id2Meas readTimePoint(const storage::QueryTimePoint &qi) {
    Id2Meas result{};
    auto clbk_lambda = [&result](const ReadResult *parent, const Meas &m) {
      if (!parent->is_closed) {
        result[m.id] = m;
      }
    };
    ReadResult::callback clbk = clbk_lambda;
    auto qres = readTimePoint(qi, clbk);
    qres->wait();
    return result;
  }

  ReadResult_ptr currentValue(const IdArray &ids, const Flag &flag, ReadResult::callback &clbk) {
	  _locker.lock();
	  auto cur_id = _query_num;
	  _query_num += 1;
	  _locker.unlock();

	  auto qres = std::make_shared<ReadResult>();
	  qres->locker.lock();
	  qres->id = cur_id;
	  qres->kind = DATA_KINDS::CURRENT_VALUE;

	  auto nd = this->_pool.construct(DATA_KINDS::CURRENT_VALUE);

	  auto p_header = reinterpret_cast<QueryCurrentValue_header *>(nd->data);
	  nd->size = sizeof(QueryCurrentValue_header);
	  p_header->id = cur_id;
	  p_header->flag = flag;

	  auto id_size = sizeof(Id) * ids.size();
	  if ((id_size + nd->size) > NetData::MAX_MESSAGE_SIZE) {
		  _pool.free(nd);
          THROW_EXCEPTION("client: query to big");
	  }
	  p_header->ids_count = (uint16_t)(ids.size());
	  auto ids_ptr = ((char *)(&p_header->ids_count) + sizeof(p_header->ids_count));
	  memcpy(ids_ptr, ids.data(), id_size);
	  nd->size += static_cast<NetData::MessageSize>(id_size);

	  qres->is_closed = false;
	  qres->clbk = clbk;
	  this->_query_results[qres->id] = qres;

	  _async_connection->send(nd);
	  return qres;
  }
  
  Id2Meas currentValue(const IdArray &ids, const Flag &flag) {
	  Id2Meas result{};
	  auto clbk_lambda = [&result](const ReadResult *parent, const Meas &m) {
		  if (!parent->is_closed) {
			  result[m.id] = m;
		  }
	  };
	  ReadResult::callback clbk = clbk_lambda;
	  auto qres = currentValue(ids, flag, clbk);
	  qres->wait();
	  return result;
  }

  ReadResult_ptr subscribe(const IdArray &ids, const Flag &flag, ReadResult::callback &clbk) {
	  _locker.lock();
	  auto cur_id = _query_num;
	  _query_num += 1;
	  _locker.unlock();

	  auto qres = std::make_shared<ReadResult>();
	  qres->locker.lock();
	  qres->id = cur_id;
	  qres->kind = DATA_KINDS::SUBSCRIBE;

	  auto nd = this->_pool.construct(DATA_KINDS::SUBSCRIBE);

	  auto p_header = reinterpret_cast<QuerSubscribe_header *>(nd->data);
	  nd->size = sizeof(QuerSubscribe_header);
	  p_header->id = cur_id;
	  p_header->flag = flag;

	  auto id_size = sizeof(Id) * ids.size();
	  if ((id_size + nd->size) > NetData::MAX_MESSAGE_SIZE) {
		  _pool.free(nd);
          THROW_EXCEPTION("client: query to big");
	  }
	  p_header->ids_count = (uint16_t)(ids.size());
	  auto ids_ptr = ((char *)(&p_header->ids_count) + sizeof(p_header->ids_count));
	  memcpy(ids_ptr, ids.data(), id_size);
	  nd->size += static_cast<NetData::MessageSize>(id_size);

	  qres->is_closed = false;
	  qres->clbk = clbk;
	  this->_query_results[qres->id] = qres;

	  _async_connection->send(nd);
	  return qres;
  }

  void compactTo(size_t pageCount){
      _locker.lock();
      auto cur_id = _query_num;
      _query_num += 1;
      _locker.unlock();
      auto nd = this->_pool.construct(DATA_KINDS::COMPACT);

      auto p_header = reinterpret_cast<QuerCompact_header *>(nd->data);
      nd->size = sizeof(QuerCompact_header);
      p_header->id = cur_id;
      p_header->pageCount=pageCount;
      p_header->from=p_header->to=Time(0);
      _async_connection->send(nd);
  }

  void compactbyTime(dariadb::Time from, dariadb::Time to){
      _locker.lock();
      auto cur_id = _query_num;
      _query_num += 1;
      _locker.unlock();
      auto nd = this->_pool.construct(DATA_KINDS::COMPACT);

      auto p_header = reinterpret_cast<QuerCompact_header *>(nd->data);
      nd->size = sizeof(QuerCompact_header);
      p_header->id = cur_id;
      p_header->pageCount=size_t(0);
      p_header->from=from;
      p_header->to=to;
      _async_connection->send(nd);
  }
  io_service _service;
  socket_ptr _socket;
  streambuf buff;

  Client::Param _params;
  utils::Locker _locker;
  std::thread _thread_handler;
  CLIENT_STATE _state;
  std::atomic_size_t _pings_answers;

  QueryNumber _query_num;
  MeasArray in_buffer_values;
  NetData_Pool _pool;
  std::map<QueryNumber, ReadResult_ptr> _query_results;
  std::shared_ptr<AsyncConnection> _async_connection;
};

Client::Client(const Param &p) : _Impl(new Client::Private(p)) {}

Client::~Client() {}

int Client::id() const {
  return _Impl->_async_connection->id();
}

void Client::connect() {
  _Impl->connect();
}

void Client::disconnect() {
  _Impl->disconnect();
}

CLIENT_STATE Client::state() const {
  return _Impl->state();
}

size_t Client::pings_answers() const {
  return _Impl->pings_answers();
}

void Client::append(const MeasArray &ma) {
  _Impl->append(ma);
}

MeasList Client::readInterval(const storage::QueryInterval &qi) {
  return _Impl->readInterval(qi);
}

ReadResult_ptr Client::readInterval(const storage::QueryInterval &qi,
                            ReadResult::callback &clbk) {
  return _Impl->readInterval(qi, clbk);
}

Id2Meas Client::readTimePoint(const storage::QueryTimePoint &qi) {
  return _Impl->readTimePoint(qi);
}

ReadResult_ptr Client::readTimePoint(const storage::QueryTimePoint &qi,
                            ReadResult::callback &clbk) {
  return _Impl->readTimePoint(qi, clbk);
}

ReadResult_ptr Client::currentValue(const IdArray &ids, const Flag &flag, ReadResult::callback &clbk) {
	return _Impl->currentValue(ids, flag,clbk);
}

Id2Meas Client::currentValue(const IdArray &ids, const Flag &flag) {
	return _Impl->currentValue(ids, flag);
}

ReadResult_ptr Client::subscribe(const IdArray &ids, const Flag &flag, ReadResult::callback &clbk) {
	return _Impl->subscribe(ids, flag, clbk);
}

void Client::compactTo(size_t pageCount){
    _Impl->compactTo(pageCount);
}

void Client::compactbyTime(dariadb::Time from, dariadb::Time to){
    _Impl->compactbyTime(from,to);
}
