#include "bytebuffer.h"
#include "../../utils/utils.h"
#include <cassert>
#include <sstream>

using namespace dariadb;
using namespace dariadb::compression;

ByteBuffer::ByteBuffer(const utils::Range &r)
    : _begin(r.begin), _end(r.end){
  _cap = static_cast<uint64_t>(std::distance(_begin, _end));
  _pos = _cap - 1;
}

ByteBuffer::~ByteBuffer() {}
