#include "crc.h"
#include <boost/crc.hpp>

uint32_t dariadb::utils::crc32(const void*buffer, const size_t size){
    boost::crc_32_type result;
    result.process_bytes(buffer,size);
    return static_cast<uint32_t>(result.checksum());
}
