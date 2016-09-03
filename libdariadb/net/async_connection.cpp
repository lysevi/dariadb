#include "async_connection.h"
#include "../utils/exception.h"
#include <cassert>
#include <functional>

using namespace std::placeholders;

using namespace boost::asio;

using namespace dariadb;
using namespace dariadb::net;

NetData::NetData(const std::string &s) {
  if (s.size() > MAX_MESSAGE_SIZE) {
    THROW_EXCEPTION_SS("s.size() > MAX_MESSAGE_SIZE - " << s.size() << ' '
                                                        << MAX_MESSAGE_SIZE);
  }
  data = new uint8_t[s.size()];
  memset(data, 0, s.size());
  memcpy(data, s.data(), s.size());
  size = static_cast<MessageSize>(s.size());
}

NetData::NetData(NetData::MessageSize s, uint8_t *d) {
  if (s > MAX_MESSAGE_SIZE) {
    THROW_EXCEPTION_SS("size > MAX_MESSAGE_SIZE - " << s << ' '
                                                    << MAX_MESSAGE_SIZE);
  }
  size = s;
  data = d;
}

NetData::~NetData() { delete[] data; }

std::tuple<NetData::MessageSize, uint8_t *> NetData::as_buffer() const {
  auto send_buffer_size = size + MARKER_SIZE;
  uint8_t *send_buffer = new uint8_t[send_buffer_size];

  // TODO use boost::object_pool
  memcpy(send_buffer, &(size), MARKER_SIZE);
  memcpy(send_buffer + MARKER_SIZE, data, size);
  return std::tie(send_buffer_size, send_buffer);
}

AsyncConnection::AsyncConnection() {
  _async_con_id = 0;
  _messages_to_send = 0;
  _is_stoped = true;
}

AsyncConnection::~AsyncConnection() noexcept(false) { full_stop(); }

void AsyncConnection::start(const socket_ptr &sock) {
  if (!_is_stoped) {
    return;
  }
  _sock = sock;
  _is_stoped = false;
  _begin_stoping_flag = false;
  readNextAsync();
}

void AsyncConnection::readNextAsync() {
  if (auto spt = _sock.lock()) {
    spt->async_read_some(
        buffer(this->marker_read_buffer, MARKER_SIZE),
        std::bind(&AsyncConnection::onReadMarker, this, _1, _2));
  }
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
    auto send_buffer=std::get<1>(ds);
    auto send_buffer_size=std::get<0>(ds);

    if (auto spt = _sock.lock()) {
      _messages_to_send++;
      async_write(
          *spt.get(), buffer(send_buffer, send_buffer_size),
          std::bind(&AsyncConnection::onDataSended, this, send_buffer, _1, _2));
    }
  }
}

void AsyncConnection::onDataSended(uint8_t *buffer,
                                   const boost::system::error_code &err,
                                   size_t read_bytes) {
  logger_info("AsyncConnection::onDataSended #", _async_con_id, " readed ",
              read_bytes);
  _messages_to_send--;
  assert(_messages_to_send >= 0);
  delete[] buffer;
  if (err) {
    this->onNetworkError(err);
  }
}

void AsyncConnection::onReadMarker(const boost::system::error_code &err,
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
    NetData::MessageSize *data_size_ptr =
        reinterpret_cast<NetData::MessageSize *>(marker_read_buffer);

    data_read_buffer_size = *data_size_ptr;
    data_read_buffer = new uint8_t[data_read_buffer_size];
    if (auto spt = _sock.lock()) {
      // TODO sync or async?. if sync - refact: rename onDataRead.
      boost::system::error_code ec;
      auto readed_bytes = spt->read_some(
          buffer(this->data_read_buffer, data_read_buffer_size), ec);
      onReadData(ec, readed_bytes);
      //      spt->async_read_some(
      //          buffer(this->data_read_buffer, data_read_buffer_size),
      //          std::bind(&AsyncConnection::onReadData, this, _1, _2));
    }
  }
}

void AsyncConnection::onReadData(const boost::system::error_code &err,
                                 size_t read_bytes) {
  logger_info("AsyncConnection::onReadData #", _async_con_id, " readed ",
              read_bytes);
  if (err) {
    this->onNetworkError(err);
  } else {
    bool cancel_flag = false;
    NetData_ptr d =
        std::make_shared<NetData>(data_read_buffer_size, data_read_buffer);
    this->data_read_buffer = nullptr;
    this->data_read_buffer_size = 0;
    try {
      this->onDataRecv(d, cancel_flag);
    } catch (std::exception &ex) {
      THROW_EXCEPTION_SS("exception on async readData. #"
                         << _async_con_id << " - " << ex.what());
    }
    if (!cancel_flag) {
      readNextAsync();
    }
  }
}
