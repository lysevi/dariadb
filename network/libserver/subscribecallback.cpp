#include <libserver/subscribecallback.h>

using namespace dariadb;
using namespace dariadb::net;

void SubscribeCallback::send_buffer(const Meas &m) {
  auto nd = std::make_shared<NetData>(DATA_KINDS::APPEND);
  nd->size = sizeof(QueryAppend_header);

  auto hdr = reinterpret_cast<QueryAppend_header *>(&nd->data);
  hdr->id = _query_num;
  size_t space_left = 0;
  QueryAppend_header::make_query(hdr, &m, size_t(1), 0, &space_left);

  auto size_to_write = NetData::MAX_MESSAGE_SIZE - MARKER_SIZE - space_left;
  nd->size = static_cast<NetData::MessageSize>(size_to_write);

  _parent->_async_connection->send(nd);
}