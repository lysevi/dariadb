#pragma once

#include "../utils/exception.h"
#include "../utils/locker.h"
#include "net_common.h"
#include "net_data.h"
#include "socket_ptr.h"
#include <atomic>
#include <memory>
#include <numeric>

namespace dariadb {
namespace net {

class AsyncConnection {
public:
  AsyncConnection(NetData_Pool *pool);
  virtual ~AsyncConnection() noexcept(false);
  void set_pool(NetData_Pool *pool);
  NetData_Pool *get_pool() { return _pool; }
  void send(const NetData_ptr &d);
  void start(const socket_ptr &sock);
  void mark_stoped();
  void full_stop(); /// stop thread, clean queue

  /// if method set 'cancel' to true, then read loop stoping.
  /// if dont_free_memory, then free NetData_ptr is in client side.
  virtual void onDataRecv(const NetData_ptr &d, bool &cancel, bool &dont_free_memory) = 0;
  virtual void onNetworkError(const boost::system::error_code &err) = 0;

  void set_id(int id) { _async_con_id = id; }
  int id() const { return _async_con_id; }
  int queue_size() const { return _messages_to_send; }

private:
  void readNextAsync();

  void onDataSended(NetData_ptr &d, const boost::system::error_code &err,
                    size_t read_bytes);
  void onReadMarker(NetData_ptr &d, const boost::system::error_code &err,
                    size_t read_bytes);
  void onReadData(NetData_ptr &d, const boost::system::error_code &err,
                  size_t read_bytes);

private:
  std::atomic_int _messages_to_send;
  int _async_con_id; // TODO just for logging. remove after release.
  socket_weak _sock;

  bool _is_stoped;
  std::atomic_bool _begin_stoping_flag;
  NetData_Pool *_pool;
};
}
}
