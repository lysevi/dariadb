#include <libdariadb/aggregate/aggregator.h>
#include <libdariadb/aggregate/timer.h>
#include <libdariadb/dariadb.h>
#include <libdariadb/timeutil.h>
#include <gtest/gtest.h>

class Aggregate : public testing::Test {
protected:
  virtual void SetUp() {
    _storage = dariadb::memory_only_storage();
    _scheme = dariadb::scheme::Scheme::create(_storage->settings());
  }

  virtual void TearDown() {
    _scheme = nullptr;
    _storage = nullptr;
  }

  /*void fill_storage(dariadb::Time from, dariadb::Time to, dariadb::Time step) {
  }*/
  dariadb::IEngine_Ptr _storage;
  dariadb::scheme::IScheme_Ptr _scheme;
};

TEST_F(Aggregate, Minute) {}
