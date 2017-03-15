#pragma once

#include <libdariadb/meas.h>
#include <libdariadb/stat.h>
#include <libdariadb/utils/async/locker.h>
#include <common/net_common.h>
#include <tuple>

#include <boost/pool/object_pool.hpp>

#include <common/net_cmn_exports.h>

namespace dariadb {
namespace net {

#pragma pack(push, 1)
struct NetData {
  typedef uint16_t MessageSize;
  static const size_t MAX_MESSAGE_SIZE = std::numeric_limits<MessageSize>::max();
  MessageSize size;
  uint8_t data[MAX_MESSAGE_SIZE];

  CM_EXPORT NetData();
  CM_EXPORT NetData(const DATA_KINDS &k);
  CM_EXPORT ~NetData();

  CM_EXPORT std::tuple<MessageSize, uint8_t *> as_buffer();
};

struct Query_header {
  uint8_t kind;
};
struct QueryHello_header {
  uint8_t kind;
  uint32_t version;
  uint32_t host_size;
};
struct QueryOk_header {
  uint8_t kind;
  QueryNumber id;
};
struct QueryError_header {
  uint8_t kind;
  QueryNumber id;
  uint16_t error_code;
};
struct QueryHelloFromServer_header {
  uint8_t kind;
  QueryNumber id;
};
struct QueryAppend_header {
  uint8_t kind;
  QueryNumber id;
  uint32_t count;
  /**
  hdr - target header to fill
  m_array - array with measurements
  size - length of m_array
  pos - position in m_array where processing must start.
  space_left - space left in buffer after processing
  return - count processed meases;
  */
  CM_EXPORT static uint32_t make_query(QueryAppend_header *hdr, const Meas *m_array,
                                       size_t size, size_t pos, size_t *space_left);
  CM_EXPORT MeasArray read_measarray() const;
};

struct QueryInterval_header {
  uint8_t kind;
  QueryNumber id;
  Time from;
  Time to;
  Flag flag;
  uint16_t ids_count;
};

struct QueryTimePoint_header {
  uint8_t kind;
  QueryNumber id;
  Time tp;
  Flag flag;
  uint16_t ids_count;
};

struct QueryCurrentValue_header {
  uint8_t kind;
  QueryNumber id;
  Flag flag;
  uint16_t ids_count;
};

struct QuerSubscribe_header {
  uint8_t kind;
  QueryNumber id;
  Flag flag;
  uint16_t ids_count;
};

struct QuerRepack_header {
  uint8_t kind;
  QueryNumber id;
};

struct QueryStat_header {
  uint8_t kind;
  QueryNumber id;
  Id meas_id;
  Time from;
  Time to;
};

struct QueryStatResult_header {
  uint8_t kind;
  QueryNumber id;
  Statistic result;
};
#pragma pack(pop)

struct NetData_Pool {
	//TODO with jemalloc pool not needed.
  CM_EXPORT void free(NetData *nd);
  CM_EXPORT NetData *construct();

  template <class T> NetData *construct(T &&a) {
	  return new NetData(a);
  }
};
using NetData_ptr = NetData*;

//struct NetData_Pool {
//	utils::async::Locker _locker;
//	typedef boost::object_pool<NetData> Pool;
//	Pool _pool;
//
//	CM_EXPORT void free(Pool::element_type *nd);
//	CM_EXPORT Pool::element_type *construct();
//
//	template <class T> Pool::element_type *construct(T &&a) {
//		_locker.lock();
//		auto res = _pool.construct(a);
//		_locker.unlock();
//		return res;
//	}
//};
//using NetData_ptr = NetData_Pool::Pool::element_type *;
const size_t MARKER_SIZE = sizeof(NetData::MessageSize);
}
}
