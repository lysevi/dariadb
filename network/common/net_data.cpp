#include <common/net_data.h>
#include <libdariadb/utils/exception.h>

using namespace dariadb;
using namespace dariadb::net;

NetData::NetData() {
  memset(data, 0, MAX_MESSAGE_SIZE);
  size = 0;
}

NetData::NetData(dariadb::net::messages::QueryKind kind, int32_t id) {
  memset(data, 0, MAX_MESSAGE_SIZE);
  size = NetData::MAX_MESSAGE_SIZE - MARKER_SIZE;

  dariadb::net::messages::QueryHeader qhdr_answer;
  qhdr_answer.set_id(id);
  qhdr_answer.set_kind(kind);

  if (!qhdr_answer.SerializeToArray(data, size)) {
    THROW_EXCEPTION("NetData::NetData message serialize error.");
  }

  size = qhdr_answer.ByteSize();
}
NetData::~NetData() {}

std::tuple<NetData::MessageSize, uint8_t *> NetData::as_buffer() {
  uint8_t *v = reinterpret_cast<uint8_t *>(this);
  auto buf_size = static_cast<MessageSize>(MARKER_SIZE + size);
  return std::tie(buf_size, v);
}
