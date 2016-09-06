#include "async_connection.h"
#include "../utils/exception.h"
#include <cassert>
#include <functional>

using namespace std::placeholders;

using namespace boost::asio;

using namespace dariadb;
using namespace dariadb::net;

AsyncConnection::AsyncConnection(NetData_Pool *pool) {
	_pool = pool;
  _async_con_id = 0;
  _messages_to_send = 0;
  _is_stoped = true;
}

AsyncConnection::~AsyncConnection() noexcept(false) { full_stop(); }

void AsyncConnection::set_pool(NetData_Pool *pool) {
	_pool = pool;
}

void AsyncConnection::start(const socket_ptr &sock) {
  if (!_is_stoped) {
    return;
  }
  _sock = sock;
  _is_stoped = false;
  _begin_stoping_flag = false;
  readNextAsync();
}

void AsyncConnection::mark_stoped() { _begin_stoping_flag = true; }

void AsyncConnection::full_stop() {
  mark_stoped();
  try {
    if (auto spt = _sock.lock()) {
      if (spt->is_open()) {
        spt->cancel();
      }
    }
  } catch (...) {
  }
}

void AsyncConnection::send(const NetData_ptr &d) {
  if (!_begin_stoping_flag) {
    auto ds = d->as_buffer();
    auto send_buffer = std::get<1>(ds);
    auto send_buffer_size = std::get<0>(ds);

    if (auto spt = _sock.lock()) {
      _messages_to_send++;
      async_write(
          *spt.get(), buffer(send_buffer, send_buffer_size),
          std::bind(&AsyncConnection::onDataSended, this, d, _1, _2));
    }
  }
}

void AsyncConnection::onDataSended(NetData_ptr &d,
                                   const boost::system::error_code &err,
                                   size_t read_bytes) {
  logger_info("AsyncConnection::onDataSended #", _async_con_id, " readed ",
              read_bytes);
  _messages_to_send--;
  assert(_messages_to_send >= 0);
  if (err) {
    this->onNetworkError(err);
  }
  _pool->free(d);
}

void AsyncConnection::readNextAsync() {
	if (auto spt = _sock.lock()) {
		NetData_ptr d = this->_pool->construct();
		spt->async_read_some(
			buffer(reinterpret_cast<uint8_t*>(&d->size), MARKER_SIZE),
			std::bind(&AsyncConnection::onReadMarker, this, d, _1, _2));
	}
}

void AsyncConnection::onReadMarker(NetData_ptr&d,const boost::system::error_code &err,
                                   size_t read_bytes) {
  logger_info("AsyncConnection::onReadMarker #", _async_con_id, " readed ",
              read_bytes);
  if (err) {
    this->onNetworkError(err);
  } else {
    if (read_bytes != MARKER_SIZE) {
      THROW_EXCEPTION_SS("AsyncConnection::onReadMarker #"
                         << _async_con_id << " - wrong marker size: expected "
                         << MARKER_SIZE << " readed " << read_bytes);
    }

    if (auto spt = _sock.lock()) {
      // TODO sync or async?. if sync - refact: rename onDataRead.
      boost::system::error_code ec;
      auto readed_bytes = spt->read_some(buffer(d->data, d->size), ec);
      onReadData(d, ec, readed_bytes);
      //      spt->async_read_some(
      //          buffer(this->data_read_buffer, data_read_buffer_size),
      //          std::bind(&AsyncConnection::onReadData, this, _1, _2));
    }
  }
}

void AsyncConnection::onReadData(NetData_ptr &d,
                                 const boost::system::error_code &err,
                                 size_t read_bytes) {
  logger_info("AsyncConnection::onReadData #", _async_con_id, " readed ",
              read_bytes);
  if (err) {
    this->onNetworkError(err);
  } else {
    bool cancel_flag = false;

    try {
      this->onDataRecv(d, cancel_flag);
    } catch (std::exception &ex) {
      THROW_EXCEPTION_SS("exception on async readData. #"
                         << _async_con_id << " - " << ex.what());
    }
	_pool->free(d);

	if (!cancel_flag) {
      readNextAsync();
    }
  }
}
