#include <libdariadb/utils/exception.h>
#include <cassert>
#include <common/async_connection.h>
#include <functional>

using namespace std::placeholders;

using namespace boost::asio;

using namespace dariadb;
using namespace dariadb::net;

AsyncConnection::AsyncConnection(NetData_Pool *pool, onDataRecvHandler onRecv,
                                 onNetworkErrorHandler onErr) {
  _pool = pool;
  _async_con_id = 0;
  _messages_to_send = 0;
  _is_stoped = true;
  _on_recv_hadler = onRecv;
  _on_error_handler = onErr;
}

AsyncConnection::~AsyncConnection() noexcept(false) {
  full_stop();
}

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

void AsyncConnection::mark_stoped() {
  _begin_stoping_flag = true;
}

void AsyncConnection::full_stop() {
  mark_stoped();
  try {
    if (auto spt = _sock.lock()) {
      if (spt->is_open()) {
        spt->close();
      }
    }
  } catch (...) {
  }
}

void AsyncConnection::send(const NetData_ptr &d) {
  if (!_begin_stoping_flag) {
    auto ptr = shared_from_this();

    auto ds = d->as_buffer();
    auto send_buffer = std::get<1>(ds);
    auto send_buffer_size = std::get<0>(ds);

    if (auto spt = _sock.lock()) {
      _messages_to_send++;
      auto buf = buffer(send_buffer, send_buffer_size);
      async_write(*spt.get(), buf, [ptr, d](auto err, auto read_bytes) {
        // logger("AsyncConnection::onDataSended #", _async_con_id, " readed ",
        // read_bytes);
        ptr->_messages_to_send--;
        assert(ptr->_messages_to_send >= 0);
        if (err) {
          ptr->_on_error_handler(err);
        }
        ptr->_pool->free(d);
      });
    }
  }
}

void AsyncConnection::readNextAsync() {
  using boost::system::error_code;
  if (auto spt = _sock.lock()) {
    auto ptr = shared_from_this();
    NetData_ptr d = this->_pool->construct();
    async_read(*spt.get(), buffer((uint8_t *)(&d->size), MARKER_SIZE),
               [ptr, d, spt](auto err, auto read_bytes) {
                 if (err) {
                   ptr->_pool->free(d);
                   if (err == boost::asio::error::operation_aborted) {
                     return;
                   }
                   ptr->_on_error_handler(err);
                 } else {
                   if (read_bytes != MARKER_SIZE) {
                     THROW_EXCEPTION("exception on async readMarker. #",
                                     ptr->_async_con_id,
                                     " - wrong marker size: expected ", MARKER_SIZE,
                                     " readed ", read_bytes);
                   }
                   auto buf = buffer((uint8_t *)(&d->data), d->size);
                   async_read(*spt.get(), buf, [ptr, d](auto err, auto read_bytes) {
                     // logger("AsyncConnection::onReadData #", _async_con_id,
                     // "
                     // readed ", read_bytes);
                     if (err) {
                       ptr->_on_error_handler(err);
                     } else {
                       bool cancel_flag = false;
                       bool dont_free_mem = false;
                       try {
                         ptr->_on_recv_hadler(d, cancel_flag, dont_free_mem);
                       } catch (std::exception &ex) {
                         THROW_EXCEPTION("exception on async readData. #",
                                         ptr->_async_con_id, " - ", ex.what());
                       }

                       if (!dont_free_mem) {
                         ptr->_pool->free(d);
                       }

                       if (!cancel_flag) {
                         ptr->readNextAsync();
                       }
                     }
                   });
                 }
               });
  }
}
