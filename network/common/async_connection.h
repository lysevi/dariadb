#pragma once

#include <libdariadb/utils/async/locker.h>
#include <libdariadb/utils/exception.h>
#include <atomic>
#include <common/net_cmn_exports.h>
#include <common/net_common.h>
#include <common/net_data.h>
#include <common/socket_ptr.h>
#include <functional>
#include <memory>

namespace dariadb {
namespace net {

class AsyncConnection : public std::enable_shared_from_this<AsyncConnection> {
public:
  /// if method set 'cancel' to true, then read loop stoping.
  /// if dont_free_memory, then free NetData_ptr is in client side.
  using onDataRecvHandler = std::function<void(const NetData_ptr &d, bool &cancel)>;
  using onNetworkErrorHandler = std::function<void(const boost::system::error_code &err)>;

public:
  CM_EXPORT AsyncConnection(onDataRecvHandler onRecv, onNetworkErrorHandler onErr);
  CM_EXPORT ~AsyncConnection() noexcept(false);
  CM_EXPORT void send(const NetData_ptr &d);
  CM_EXPORT void start(const socket_ptr &sock);
  CM_EXPORT void mark_stoped();
  CM_EXPORT void full_stop(); /// stop thread, clean queue

  void set_id(int id) { _async_con_id = id; }
  int id() const { return _async_con_id; }
  int queue_size() const { return _messages_to_send; }

private:
  void readNextAsync();

private:
  std::atomic_int _messages_to_send;
  int _async_con_id;
  socket_weak _sock;

  bool _is_stoped;
  std::atomic_bool _begin_stoping_flag;

  onDataRecvHandler _on_recv_hadler;
  onNetworkErrorHandler _on_error_handler;
};
}
}
