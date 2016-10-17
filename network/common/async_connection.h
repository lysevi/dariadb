#pragma once

#include <libdariadb/utils/exception.h>
#include <libdariadb/utils/locker.h>
#include <common/net_common.h>
#include <common/net_data.h>
#include <common/socket_ptr.h>
#include <atomic>
#include <memory>
#include <functional>

#include <common/dariadb_net_cmn_exports.h>

namespace dariadb {
namespace net {

class AsyncConnection: public std::enable_shared_from_this<AsyncConnection>{
public:
	/// if method set 'cancel' to true, then read loop stoping.
	/// if dont_free_memory, then free NetData_ptr is in client side.
	using onDataRecvHandler = std::function<void(const NetData_ptr &d, bool &cancel, bool &dont_free_memory)>;
	using onNetworkErrorHandler = std::function<void(const boost::system::error_code &err)>;
public:
  DARIADBNET_CMN_EXPORTS AsyncConnection(NetData_Pool *pool, onDataRecvHandler onRecv, onNetworkErrorHandler onErr);
  DARIADBNET_CMN_EXPORTS ~AsyncConnection() noexcept(false);
  DARIADBNET_CMN_EXPORTS void set_pool(NetData_Pool *pool);
  DARIADBNET_CMN_EXPORTS NetData_Pool *get_pool() { return _pool; }
  DARIADBNET_CMN_EXPORTS void send(const NetData_ptr &d);
  DARIADBNET_CMN_EXPORTS void start(const socket_ptr &sock);
  DARIADBNET_CMN_EXPORTS void mark_stoped();
  DARIADBNET_CMN_EXPORTS void full_stop(); /// stop thread, clean queue

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
  NetData_Pool *_pool;

  onDataRecvHandler _on_recv_hadler;
  onNetworkErrorHandler _on_error_handler;
};
}
}
