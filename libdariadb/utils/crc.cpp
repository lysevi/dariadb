#include "crc.h"
#include <boost/crc.hpp>

size_t dariadb::utils::crc32(const void*buffer, const size_t size){
    boost::crc_32_type result;
    result.process_bytes(buffer,size);
    return result.checksum();
}
