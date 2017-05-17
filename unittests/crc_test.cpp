
#include "helpers.h"
#include <libdariadb/utils/crc.h>
#include <catch.hpp>

TEST_CASE("CrcTest.NotEqualToSource") {
  int value;
  char *buffer;
  size_t size;

  value = 0;
  buffer = (char *)&value;
  size = sizeof(value);
  auto crc32_1 = dariadb::utils::crc32(buffer, size);

  EXPECT_NE(crc32_1, uint32_t(value));

  value = 0;
  buffer = nullptr;
  size = 0;
}
