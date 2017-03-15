#include <libdariadb/compression/xor.h>
#include <common/net_data.h>
#include <cstring>

using namespace dariadb;
using namespace dariadb::net;

namespace netdata_inner {
#pragma pack(push, 1)
struct PackHeader {
  Id id;
  uint16_t count;
};

const size_t PACKED_BUFFER_MAX_SIZE =
    sizeof(dariadb::Flag) + sizeof(dariadb::Time) + sizeof(dariadb::Value);
struct PackMeas {
  uint8_t packed[PACKED_BUFFER_MAX_SIZE];
  size_t size;
  PackMeas(dariadb::Flag flg, dariadb::Time tm, dariadb::Value val) {
    memset(packed, 0, PACKED_BUFFER_MAX_SIZE);
    size = 0;
    pack(flg);
    pack(tm);
    pack(compression::inner::flat_double_to_int(val));
  }
  /// LEB128
  template <typename T> void pack(const T v) {
    auto x = v;
    do {
      auto sub_res = x & 0x7fU;
      if (x >>= 7)
        sub_res |= 0x80U;
      packed[size] = static_cast<uint8_t>(sub_res);
      ++size;
    } while (x);
  }
};

struct UnpackMeas {
  uint8_t *ptr;
  dariadb::Flag flag;
  dariadb::Time time;
  dariadb::Value value;
  UnpackMeas(uint8_t *_ptr) {
    ptr = _ptr;
    flag = unpack<dariadb::Flag>();
    time = unpack<dariadb::Time>();
    value = dariadb::compression::inner::flat_int_to_double(unpack<uint64_t>());
  }

  template <typename T> T unpack() {
    T result = T();

    size_t bytes = 0;
    while (true) {
      auto readed = *ptr;
      result |= (readed & 0x7fULL) << (7 * bytes++);
      ++ptr;
      if (!(readed & 0x80U)) {
        break;
      }
    }
    return result;
  }
};
#pragma pack(pop)
}

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

uint32_t QueryAppend_header::make_query(QueryAppend_header *hdr, const Meas *m_array,
                                        size_t size, size_t pos, size_t *space_left) {
  using namespace netdata_inner;
  uint32_t result = 0;
  auto free_space =
      (NetData::MAX_MESSAGE_SIZE - MARKER_SIZE - 1 - sizeof(QueryAppend_header));

  auto ptr = ((char *)(&hdr->count) + sizeof(hdr->count)); // first byte after header
  PackHeader *pack = (PackHeader *)ptr;
  pack->id = m_array[pos].id;
  pack->count = 0;
  ptr += sizeof(PackHeader);

  auto end = (char *)(hdr) + free_space;

  while (pos < size && ptr != end) {
    netdata_inner::PackMeas sm{m_array[pos].flag, m_array[pos].time, m_array[pos].value};
    auto bytes_left = (size_t)(end - ptr);
    assert(bytes_left < NetData::MAX_MESSAGE_SIZE);
    if (m_array[pos].id != pack->id) {
      if (bytes_left <= (sizeof(PackHeader) + sm.size)) {
        break;
      }
      pack = (PackHeader *)ptr;
      pack->id = m_array[pos].id;
      pack->count = 0;
      ptr += sizeof(PackHeader);
    }
    if (bytes_left <= sm.size) {
      break;
    }

    std::memcpy(ptr, sm.packed, sm.size);
    ptr += sm.size;
    assert(ptr < end);
    ++pack->count;
    ++pos;
    ++result;
  }
  hdr->count = result;
  *space_left = (size_t)(end - ptr);

  return result;
}

MeasArray QueryAppend_header::read_measarray() const {
  using namespace netdata_inner;

  size_t array_size = size_t(count);
  MeasArray ma(array_size);
  size_t pos = 0;
  auto ptr = ((uint8_t *)(&count) + sizeof(count)); // first byte after header

  while (pos < count) {
    PackHeader *pack = (PackHeader *)ptr;
    ptr += sizeof(PackHeader);
    assert(pack->count <= count);
    for (uint16_t i = 0; i < pack->count; ++i) {
      UnpackMeas sm(ptr);

      ma[pos].id = pack->id;
      ma[pos].flag = sm.flag;
      ma[pos].value = sm.value;
      ma[pos].time = sm.time;
      ++pos;
      ptr = sm.ptr;
    }
  }
  return ma;
}

void NetData_Pool::free(NetData *nd) {
	delete nd;
}

NetData *NetData_Pool::construct() {
	return new NetData;
}
//TODO with jemalloc pool not needed.
//void NetData_Pool::free(NetData *nd) {
//	delete nd;
//}
//
//NetData *NetData_Pool::construct() {
//	return new NetData;
//}
