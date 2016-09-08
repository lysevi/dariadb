#pragma once

#include "net_common.h"
#include "../meas.h"
#include <tuple>

#include <boost/pool/object_pool.hpp>

namespace dariadb {
	namespace net {

#pragma pack(push, 1)
		struct NetData {
			typedef uint16_t MessageSize;
			static const size_t MAX_MESSAGE_SIZE = std::numeric_limits<MessageSize>::max();
			MessageSize size;
			uint8_t data[MAX_MESSAGE_SIZE];

			NetData();
			NetData(const DataKinds&k);
			~NetData();

			std::tuple<MessageSize, uint8_t*> as_buffer();
		};
		typedef uint32_t QueryNumber;
		struct Query_header {
			uint8_t kind;
		};
		struct QueryOk_header {
			uint8_t kind;
			QueryNumber id;
		};
		struct QueryHelloFromServer_header {
			uint8_t kind;
			QueryNumber id;
		};
		struct QueryWrite_header {
			uint8_t kind;
			QueryNumber id;
			uint32_t count;
		};
		struct QueryInterval_header{
			uint8_t kind;
			QueryNumber id;
			Time from;
			Time to;
			Flag flag;
			Flag source;
			uint16_t ids_count;
		};
#pragma pack(pop)

		using NetData_Pool = boost::object_pool<NetData>;
		using NetData_ptr = NetData_Pool::element_type*;

		const size_t MARKER_SIZE = sizeof(NetData::MessageSize);
	}
}