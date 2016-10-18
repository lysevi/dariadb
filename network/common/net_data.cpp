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
