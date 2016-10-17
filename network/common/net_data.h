#pragma once

#include <libdariadb/meas.h>
#include <libdariadb/utils/locker.h>
#include <common/net_common.h>
#include <tuple>

#include <boost/pool/object_pool.hpp>

#include <common/dariadb_net_cmn_exports.h>

namespace dariadb {
namespace net {

#pragma pack(push, 1)
struct NetData {
  typedef uint16_t MessageSize;
  static const size_t MAX_MESSAGE_SIZE = std::numeric_limits<MessageSize>::max();
  MessageSize size;
  uint8_t data[MAX_MESSAGE_SIZE];

  DARIADBNET_CMN_EXPORTS NetData();
  DARIADBNET_CMN_EXPORTS NetData(const DATA_KINDS &k);
  DARIADBNET_CMN_EXPORTS ~NetData();

  DARIADBNET_CMN_EXPORTS std::tuple<MessageSize, uint8_t *> as_buffer();
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

  DARIADBNET_CMN_EXPORTS MeasArray read_measarray() const;
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
#pragma pack(pop)

//using NetData_Pool = boost::object_pool<NetData>;
struct NetData_Pool {
	utils::Locker _locker;
	typedef boost::object_pool<NetData> Pool;
	Pool _pool;

    DARIADBNET_CMN_EXPORTS void free(Pool::element_type*nd);
    DARIADBNET_CMN_EXPORTS Pool::element_type* construct();

	template<class T>
	Pool::element_type* construct(T&&a) {
		_locker.lock();
		auto res = _pool.construct(a);
		_locker.unlock();
		return res;
	}
};
using NetData_ptr = NetData_Pool::Pool::element_type*;

const size_t MARKER_SIZE = sizeof(NetData::MessageSize);
}
}
