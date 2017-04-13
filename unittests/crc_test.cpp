#include <libdariadb/utils/crc.h>
#include <gtest/gtest.h>

class CrcTest : public testing::Test {
protected:
  virtual void SetUp() {
    value = 0;
    buffer = (char *)&value;
    size = sizeof(value);
  }

  virtual void TearDown() {
    value = 0;
    buffer = nullptr;
    size = 0;
  }

  int value;
  char *buffer;
  size_t size;
};

TEST_F(CrcTest, NotEqualToSourc) {
  auto crc32_1 = dariadb::utils::crc32(buffer, size);

  EXPECT_NE(crc32_1, uint32_t(value));
}
