#pragma once

#include "common.h"
#include "../utils/locker.h"
#include "../utils/exception.h"
#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>
#include <list>
#include <numeric>
namespace dariadb {
namespace net {

struct NetData {
	typedef uint64_t MessageSize;
	static const size_t MAX_MESSAGE_SIZE = std::numeric_limits<MessageSize>::max();
  MessageSize size;
  uint8_t *data;
  NetData(const std::string&s) {
	  if (s.size() > MAX_MESSAGE_SIZE) {
		  THROW_EXCEPTION_SS("s.size() > MAX_MESSAGE_SIZE - " << s.size() << ' ' << MAX_MESSAGE_SIZE);
	  }
	  data = new uint8_t[s.size()];
	  memset(data, 0, s.size());
	  memcpy(data, s.data(), s.size());
	  size = static_cast<MessageSize>(s.size());
  }

  NetData(NetData::MessageSize s, uint8_t*d) {
	  if (s > MAX_MESSAGE_SIZE) {
		  THROW_EXCEPTION_SS("size > MAX_MESSAGE_SIZE - " << s << ' ' << MAX_MESSAGE_SIZE);
	  }
	  size = s;
	  data = d;
  }
  ~NetData() { delete[] data; }
};
using NetData_ptr = std::shared_ptr<NetData>;

const size_t MARKER_SIZE = sizeof(NetData::MessageSize);

class AsyncConnection {
public:
  AsyncConnection();
  virtual ~AsyncConnection() noexcept(false);
  void send(const NetData_ptr &d);
  void start(const socket_ptr &sock);
  void mark_stoped();
  void full_stop(); ///stop thread, clean queue

  virtual void onDataRecv(const NetData_ptr&d) = 0;
  virtual void onNetworkError(const boost::system::error_code&err) = 0;
  size_t queue_size()const {
	  return _queries.size() + (this->_current_query!=nullptr?1:0);
  }
  void queue_clear() {
	  std::lock_guard<std::mutex> lg(_ac_locker);
	  _queries.clear();
  }
  void set_id(int id) { _async_con_id = id; }
  int id()const { return _async_con_id; }
private:
  void queue_thread();

  void readNextAsync();

  void onDataSended(const boost::system::error_code &err, size_t read_bytes);
  void onReadMarker(const boost::system::error_code &err, size_t read_bytes);
  void onReadData(const boost::system::error_code &err, size_t read_bytes);

  //void allocate_send_buffer(MESSAGE_SI)
private:
  int _async_con_id; // TODO just for logging. remove after release.
  socket_weak _sock;
  
  std::list<NetData_ptr> _queries;
  NetData_ptr _current_query;
  
  std::mutex _ac_locker;
  std::condition_variable _cond;
  std::thread _thread_handler;
  
  char marker_buffer[MARKER_SIZE];
  char marker_read_buffer[MARKER_SIZE];
  
  uint8_t *data_send_buffer;
  NetData::MessageSize data_send_buffer_size;

  uint8_t *data_read_buffer;
  NetData::MessageSize data_read_buffer_size;

  bool _is_stoped;
  std::atomic_bool _begin_stoping_flag;
};
}
}
