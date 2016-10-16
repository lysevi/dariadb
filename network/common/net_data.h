#pragma once

#include <common/net_common.h>
#include <libdariadb/utils/locker.h>
#include <common/messages.pb.h>
#include <tuple>

#include <boost/pool/object_pool.hpp>


namespace dariadb {
namespace net {

#pragma pack(push, 1)
struct NetData {
  typedef uint16_t MessageSize;
  static const size_t MAX_MESSAGE_SIZE =
      std::numeric_limits<MessageSize>::max();
  MessageSize size;
  uint8_t data[MAX_MESSAGE_SIZE];

  NetData();
  NetData(dariadb::net::messages::QueryKind kind, int32_t id);
  ~NetData();

  std::tuple<MessageSize, uint8_t *> as_buffer();
};
#pragma pack(pop)

// using NetData_Pool = boost::object_pool<NetData>;
struct NetData_Pool {
  utils::Locker _locker;
  typedef boost::object_pool<NetData> Pool;
  Pool _pool;

  void free(Pool::element_type *nd) {
    _locker.lock();
    _pool.free(nd);
    _locker.unlock();
  }

  Pool::element_type *construct() {
    _locker.lock();
    auto res = _pool.construct();
    _locker.unlock();
    return res;
  }
  template <class T> Pool::element_type *construct(T &&a) {
    _locker.lock();
    auto res = _pool.construct(a);
    _locker.unlock();
    return res;
  }

  template <class T1,class T2> Pool::element_type *construct(T1 &&a,T2 &&b) {
    _locker.lock();
    auto res = _pool.construct(a,b);
    _locker.unlock();
    return res;
  }
};
using NetData_ptr = NetData_Pool::Pool::element_type *;

const size_t MARKER_SIZE = sizeof(NetData::MessageSize);
}
}
