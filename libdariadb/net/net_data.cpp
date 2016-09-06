#include "net_data.h"

using namespace dariadb;
using namespace dariadb::net;

NetData::NetData() {
	memset(data, 0, MAX_MESSAGE_SIZE);
	size = 0;
}

NetData::~NetData() {}

void NetData::append(const DataKinds&k) {
	data[size] = static_cast<uint8_t>(k);
	size += sizeof(DataKinds);
}

std::tuple<NetData::MessageSize, uint8_t *> NetData::as_buffer() {
	uint8_t *v = reinterpret_cast<uint8_t *>(this);
	auto buf_size = static_cast<MessageSize>(MARKER_SIZE + size);
	return std::tie(buf_size, v);
}
