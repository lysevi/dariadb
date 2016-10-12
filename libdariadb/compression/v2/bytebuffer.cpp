#include "libdariadb/compression/v2/bytebuffer.h"
#include "libdariadb/utils/utils.h"
#include <cassert>
#include <sstream>

using namespace dariadb;
using namespace dariadb::compression::v2;

ByteBuffer::ByteBuffer(const utils::Range &r) : _begin(r.begin), _end(r.end) {
  _cap = static_cast<uint32_t>(std::distance(_begin, _end));
  reset_pos();
}

ByteBuffer::~ByteBuffer() {}
