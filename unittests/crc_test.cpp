
#include "helpers.h"
#include <libdariadb/utils/crc.h>
#include <catch.hpp>

class CrcTest {
public:
  CrcTest() {
    value = 0;
    buffer = (char *)&value;
    size = sizeof(value);
  }

  ~CrcTest() {
    value = 0;
    buffer = nullptr;
    size = 0;
  }

  int value;
  char *buffer;
  size_t size;
};

TEST_CASE_METHOD(CrcTest, "NotEqualToSourc") {
  auto crc32_1 = dariadb::utils::crc32(buffer, size);

  EXPECT_NE(crc32_1, uint32_t(value));
}
