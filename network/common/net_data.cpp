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

ParsedQueryHeader NetData::readHeader() const {
  dariadb::net::messages::QueryHeader *qhd = new dariadb::net::messages::QueryHeader;
  qhd->ParseFromArray(data, size);

  ParsedQueryHeader result;
  result.id = qhd->id();
  result.kind = qhd->kind();
  result.parsed_info = qhd;
  return result;
}

void NetData::construct_hello_query(const std::string &host, const int32_t id) {
  size = NetData::MAX_MESSAGE_SIZE - MARKER_SIZE;

  dariadb::net::messages::QueryHeader qhdr;
  qhdr.set_id(0);
  qhdr.set_kind(dariadb::net::messages::HELLO);
  dariadb::net::messages::QueryHello *qhm =
      qhdr.MutableExtension(dariadb::net::messages::QueryHello::qhello);

  qhm->set_host(host);
  qhm->set_version(PROTOCOL_VERSION);

  if (!qhdr.SerializeToArray(data, size)) {
    THROW_EXCEPTION("hello message serialize error");
  }

  size = qhdr.ByteSize();
}

void NetData::construct_error(QueryNumber query_num, const ERRORS &err) {
  size = NetData::MAX_MESSAGE_SIZE - MARKER_SIZE;

  dariadb::net::messages::QueryHeader qhdr;
  qhdr.set_id(query_num);
  qhdr.set_kind(dariadb::net::messages::ERR);

  dariadb::net::messages::QueryError *qhm =
      qhdr.MutableExtension(dariadb::net::messages::QueryError::qerror);
  qhm->set_errpr_code((uint16_t)err);

  if (!qhdr.SerializeToArray(data, size)) {
    THROW_EXCEPTION("error message serialize error");
  }

  size = qhdr.ByteSize();
}

ParsedQueryHeader::~ParsedQueryHeader() {
	auto deleted_info = (dariadb::net::messages::QueryHeader *)parsed_info;
  delete deleted_info;
}

int32_t ParsedQueryHeader::error_code() const {
  auto *qhd = (dariadb::net::messages::QueryHeader *)parsed_info;
  auto qerr = qhd->GetExtension(dariadb::net::messages::QueryError::qerror);
  return qerr.errpr_code();
}

ParsedQueryHeader::Hello ParsedQueryHeader::host_name() const {
  auto *qhd = (dariadb::net::messages::QueryHeader *)parsed_info;
  auto q = qhd->GetExtension(dariadb::net::messages::QueryHello::qhello);
  ParsedQueryHeader::Hello result;
  result.host = q.host();
  result.protocol = q.version();
  return result;
}