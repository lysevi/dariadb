#pragma once

#include <cstdint>

namespace dariadb{
namespace storage{
//"dariadb."
const uint64_t MAGIC_NUMBER_DARIADB = 0x2E62646169726164;
//"IndexFtr"
const uint64_t MAGIC_NUMBER_INDEXFTR = 0x7274467865646e49;
//"PageFtr."
const uint64_t MAGIC_NUMBER_INDEXFTR = 0x2e72744665676150;
}
}