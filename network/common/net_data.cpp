#include <common/net_data.h>

using namespace dariadb;
using namespace dariadb::net;

NetData::NetData() {
  memset(data, 0, MAX_MESSAGE_SIZE);
  size = 0;
}

NetData::NetData(const DATA_KINDS &k) {
  memset(data, 0, MAX_MESSAGE_SIZE);
  size = sizeof(DATA_KINDS);
  data[0] = static_cast<uint8_t>(k);
}

NetData::~NetData() {}

std::tuple<NetData::MessageSize, uint8_t *> NetData::as_buffer() {
  uint8_t *v = reinterpret_cast<uint8_t *>(this);
  auto buf_size = static_cast<MessageSize>(MARKER_SIZE + size);
  return std::tie(buf_size, v);
}

uint32_t QueryAppend_header::make_query(QueryAppend_header *hdr, const Meas*m_array, size_t size, size_t pos) {
	auto left = size - pos;
	auto cur_msg_space = (NetData::MAX_MESSAGE_SIZE - 1 - sizeof(QueryAppend_header));
	uint32_t count_to_write=0;
	
	if ((left * sizeof(Meas)) > cur_msg_space) {
		count_to_write =(uint32_t) cur_msg_space / sizeof(Meas);
	}
	else {
		count_to_write = (uint32_t)left;
	}

	auto meas_ptr = ((char *)(&hdr->count) + sizeof(hdr->count));

	auto size_to_write = count_to_write * sizeof(Meas);
	memcpy(meas_ptr, m_array + pos, size_to_write);
	hdr->count =(uint32_t)count_to_write;

	
	return count_to_write;
}

MeasArray QueryAppend_header::read_measarray() const {
  MeasArray ma{size_t(count)};
  memcpy(ma.data(), ((char *)(&count) + sizeof(count)), count * sizeof(Meas));
  return ma;
}

void NetData_Pool::free(Pool::element_type*nd) {
    _locker.lock();
    _pool.free(nd);
    _locker.unlock();
}

NetData_Pool::Pool::element_type* NetData_Pool::construct() {
    _locker.lock();
    auto res=_pool.construct();
    _locker.unlock();
    return res;
}
