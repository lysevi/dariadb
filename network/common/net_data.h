#pragma once

#include <common/messages.pb.h>
#include <common/net_common.h>
#include <libdariadb/utils/locker.h>
#include <libdariadb/meas.h>
#include <tuple>

#include <boost/pool/object_pool.hpp>

#include <common/dariadb_net_cmn_exports.h>

namespace dariadb {
namespace net {

class ParsedQueryHeader {
public:
  struct Hello {
    std::string host;
    int32_t protocol;
  };
  dariadb::net::messages::QueryKind kind;
  int32_t id;
  void *parsed_info;

  DARIADBNET_CMN_EXPORTS ~ParsedQueryHeader();
  DARIADBNET_CMN_EXPORTS int32_t error_code() const;
  DARIADBNET_CMN_EXPORTS Hello host_name() const;
};

class NetData_Pool;
#pragma pack(push, 1)
class NetData {
public:
  typedef uint16_t MessageSize;
  static const size_t MAX_MESSAGE_SIZE = std::numeric_limits<MessageSize>::max();
  MessageSize size;
  uint8_t data[MAX_MESSAGE_SIZE];

  DARIADBNET_CMN_EXPORTS NetData();
  DARIADBNET_CMN_EXPORTS NetData(dariadb::net::messages::QueryKind kind, int32_t id);
  DARIADBNET_CMN_EXPORTS void construct_hello_query(const std::string &host,
                                                    const int32_t id);
  DARIADBNET_CMN_EXPORTS void construct_error(QueryNumber query_num, const ERRORS &err);
  DARIADBNET_CMN_EXPORTS ~NetData();

  DARIADBNET_CMN_EXPORTS std::tuple<MessageSize, uint8_t *> as_buffer();
  DARIADBNET_CMN_EXPORTS ParsedQueryHeader readHeader() const;
};
#pragma pack(pop)

// using NetData_Pool = boost::object_pool<NetData>;
class NetData_Pool {
public:
  utils::Locker _locker;
  typedef boost::object_pool<NetData> Pool;
  Pool _pool;

  DARIADBNET_CMN_EXPORTS void free(Pool::element_type *nd) {
    _locker.lock();
    _pool.free(nd);
    _locker.unlock();
  }

  DARIADBNET_CMN_EXPORTS Pool::element_type *construct() {
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

  template <class T1, class T2> Pool::element_type *construct(T1 &&a, T2 &&b) {
    _locker.lock();
    auto res = _pool.construct(a, b);
    _locker.unlock();
    return res;
  }
};
using NetData_ptr = NetData_Pool::Pool::element_type *;

const size_t MARKER_SIZE = sizeof(NetData::MessageSize);
}
}
