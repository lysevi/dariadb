#pragma once

#include <cstdint>

namespace dariadb{
namespace storage{
//"dariadb."
const uint64_t MAGIC_NUMBER_DARIADB = 0x2E62646169726164;
const uint8_t MAGIC_NUMBER_INDEXFTR = 0xAA;
const uint8_t MAGIC_NUMBER_PAGEFTR = 0x55;
}
}