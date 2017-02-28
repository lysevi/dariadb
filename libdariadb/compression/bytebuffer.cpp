#include <libdariadb/compression/bytebuffer.h>
#include <libdariadb/utils/utils.h>
#include <iterator>
using namespace dariadb;
using namespace dariadb::compression;

ByteBuffer::ByteBuffer(const Range &r) : _begin(r.begin), _end(r.end) {
  _cap = static_cast<uint32_t>(std::distance(_begin, _end));
  reset_pos();
}

ByteBuffer::~ByteBuffer() {}
