#pragma once

#include "common.h"
#include "../utils/locker.h"
#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>
#include <list>

namespace dariadb {
namespace net {
struct NetData {
  uint64_t size;
  uint8_t *data;
  NetData(const std::string&s) {
	  data = new uint8_t[s.size()];
	  memset(data, 0, s.size());
	  memcpy(data, s.data(), s.size());
	  size = s.size();
  }

  NetData(uint64_t s, uint8_t*d) {
	  size = s;
	  data = d;
  }
  ~NetData() { delete[] data; }
};
using NetData_ptr = std::shared_ptr<NetData>;

const size_t MARKER_SIZE = sizeof(uint64_t);

class AsyncConnection {
public:
  AsyncConnection();
  virtual ~AsyncConnection() noexcept(false);
  void send(const NetData_ptr &d);
  void start(const socket_ptr &sock);
  void stop();

  virtual void onDataRecv(const NetData_ptr&d) = 0;
  virtual void onNetworkError(const boost::system::error_code&err) = 0;
  size_t queue_size()const {
	  return _queries.size() + (this->_current_query!=nullptr?1:0);
  }
  void queue_clear() {
	  std::lock_guard<std::mutex> lg(_ac_locker);
	  _queries.clear();
  }
private:
  void queue_thread();

  void readNextAsync();

  void onMarkerSended(const boost::system::error_code &err, size_t read_bytes);
  void onDataSended(const boost::system::error_code &err, size_t read_bytes);
  
  void onReadMarker(const boost::system::error_code &err, size_t read_bytes);
  void onReadData(const boost::system::error_code &err, size_t read_bytes);
private:
  socket_weak _sock;
  std::list<NetData_ptr> _queries;
  NetData_ptr _current_query;
  std::mutex _ac_locker;
  std::condition_variable _cond;
  bool _stoped;
  std::atomic_bool _stop_flag;
  std::thread _thread_handler;
  char marker_buffer[MARKER_SIZE];
  char marker_read_buffer[MARKER_SIZE];
  uint8_t *data_read_buffer;
  uint64_t data_read_buffer_size;
};
}
}
