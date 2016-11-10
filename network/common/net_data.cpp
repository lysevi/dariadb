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
#pragma pack(push, 1)
struct PackedMeases {
	Id id;
	uint16_t count;
};

struct SubMeas {
	dariadb::Flag flg;
	dariadb::Time tm;
	dariadb::Value val;
};
#pragma pack(pop)

uint32_t QueryAppend_header::make_query(QueryAppend_header *hdr, const Meas*m_array, size_t size, size_t pos, size_t* space_left) {
	uint32_t result = 0;
	auto free_space = (NetData::MAX_MESSAGE_SIZE - MARKER_SIZE - 1 - sizeof(QueryAppend_header));

	auto ptr = ((char *)(&hdr->count) + sizeof(hdr->count)); //first byte after header
	PackedMeases* pack = (PackedMeases*)ptr;
	pack->id = m_array[pos].id;
	pack->count = 0;
	ptr += sizeof(PackedMeases);
	
	auto end = (char*)(hdr)+free_space;

	while (pos < size) {
		auto bytes_left = (size_t) (end - ptr);
		if (m_array[pos].id != pack->id) {
			if (bytes_left <= (sizeof(PackedMeases) + sizeof(SubMeas))) {
				break;
			}
			pack = (PackedMeases*)ptr;
			pack->id = m_array[pos].id;
			pack->count = 0;
			ptr += sizeof(PackedMeases);
		}
		if (bytes_left <= sizeof(SubMeas)) {
			break;
		}
		SubMeas*sm = (SubMeas*)ptr;
		sm->flg = m_array[pos].flag;
		sm->tm = m_array[pos].time;
		sm->val = m_array[pos].value;
		++pack->count;
		ptr += sizeof(SubMeas);
		++pos;
		++result;
	}
	hdr->count = result;
	*space_left = (size_t)(end - ptr);

	return result;
}

MeasArray QueryAppend_header::read_measarray() const {
  MeasArray ma{size_t(count)};
  size_t pos = 0;
  auto ptr = ((char *)(&count) + sizeof(count)); //first byte after header
  
  
  while (pos < count) {
	  PackedMeases* pack = (PackedMeases*)ptr;
	  ptr += sizeof(PackedMeases);
	  assert(pack->count <= count);
	  for (uint16_t i = 0; i < pack->count; ++i) {
		  SubMeas*sm = (SubMeas*)ptr;

		  ma[pos].id = pack->id;
		  ma[pos].flag = sm->flg;
		  ma[pos].value = sm->val;
		  ma[pos].time = sm->tm;
		  ++pos;
		  ptr += sizeof(SubMeas);
	  }
  }
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
