#pragma once

#include "common.h"
#include "../utils/locker.h"
#include "../utils/exception.h"
#include <atomic>
#include <memory>
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

  ///if method set 'cancel' to true, then read loop stoping.
  virtual void onDataRecv(const NetData_ptr&d, bool&cancel) = 0;
  virtual void onNetworkError(const boost::system::error_code&err) = 0;

  void queue_clear();
  void set_id(int id) { _async_con_id = id; }
  int id()const { return _async_con_id; }
  int queue_size()const{return _messages_to_send;}
private:
  void readNextAsync();

  void onDataSended(uint8_t* buffer,const boost::system::error_code &err, size_t read_bytes);
  void onReadMarker(const boost::system::error_code &err, size_t read_bytes);
  void onReadData(const boost::system::error_code &err, size_t read_bytes);
private:
  std::atomic_int _messages_to_send;
  int _async_con_id; // TODO just for logging. remove after release.
  socket_weak _sock;
  
  char marker_buffer[MARKER_SIZE];
  char marker_read_buffer[MARKER_SIZE];
  
  uint8_t *data_read_buffer;
  NetData::MessageSize data_read_buffer_size;

  bool _is_stoped;
  std::atomic_bool _begin_stoping_flag;
};
}
}
