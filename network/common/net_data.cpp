#include "net_data.h"

using namespace dariadb;
using namespace dariadb::net;

NetData::NetData() {
  memset(data, 0, MAX_MESSAGE_SIZE);
  size = 0;
}

NetData::NetData(const DataKinds &k) {
  memset(data, 0, MAX_MESSAGE_SIZE);
  size = sizeof(DataKinds);
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