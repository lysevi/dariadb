
#include <catch.hpp>

#include "helpers.h"

#include <libdariadb/storage/cola.h>
#include <libdariadb/storage/engine_environment.h>
#include <libdariadb/storage/manifest.h>
#include <libdariadb/utils/async/thread_manager.h>
#include <libdariadb/utils/fs.h>
#include <libdariadb/utils/logger.h>
#include <libdariadb/utils/utils.h>

TEST_CASE("COLA.IndexreadWrite") {
  using namespace dariadb::storage;
  Cola::Param p{uint8_t(3), uint8_t(5)};
  auto sz = Cola::index_size(p);
  EXPECT_GT(sz, size_t(0));

  uint8_t *buffer = new uint8_t[sz + 1];
  buffer[sz] = uint8_t(255);
  uint64_t address = 1, chunk_id = 1;
  size_t addeded = 0;
  dariadb::Time maxTime = 1;
  dariadb::Id targetId{10};
  {
    Cola c(p, targetId, buffer);
    EXPECT_EQ(buffer[sz], uint8_t(255));
    EXPECT_EQ(c.levels(), p.levels);
    EXPECT_EQ(c.targetId(), targetId);

	auto lnk = c.queryLink(maxTime);
	EXPECT_TRUE(lnk.IsEmpty());

    while (c.addLink(address, chunk_id, maxTime)) {
      addeded++;
      auto queryResult = c.queryLink(dariadb::Time(1), maxTime);
      EXPECT_EQ(addeded, queryResult.size());
      EXPECT_EQ(buffer[sz], uint8_t(255));

      bool sorted = std::is_sorted(queryResult.begin(), queryResult.end(),
                                   [](const Cola::Link &l, const Cola::Link &r) {
                                     return l.max_time < r.max_time;
                                   });
      EXPECT_TRUE(sorted);

	  lnk = c.queryLink(maxTime);
	  EXPECT_EQ(lnk.max_time , maxTime);

	  lnk = c.queryLink(maxTime+1);
	  EXPECT_EQ(lnk.max_time, maxTime);

	  if (maxTime != size_t(1)) {
		  lnk = c.queryLink(maxTime - 1);
		  EXPECT_EQ(lnk.max_time, maxTime - 1);
	  }
      address++;
      chunk_id++;
      maxTime++;
    }
    EXPECT_GT(addeded, size_t(0));
    auto queryResult = c.queryLink(dariadb::Time(1), maxTime);
    EXPECT_EQ(addeded, queryResult.size());
  }
  {
    Cola c(buffer);
    EXPECT_EQ(c.levels(), p.levels);
    EXPECT_EQ(c.targetId(), targetId);
    auto queryResult = c.queryLink(dariadb::Time(1), maxTime);
	auto size_on_start = queryResult.size();
	EXPECT_EQ(addeded, size_on_start);
	
	for (size_t i = addeded; i >= 0 && size_on_start != 0; --i) {
		c.rm(dariadb::Time(addeded), uint64_t(addeded));
		addeded--;

		queryResult = c.queryLink(dariadb::Time(1), maxTime);
		EXPECT_GT(size_on_start, queryResult.size());
		size_on_start = queryResult.size();
	}
  }
  delete[] buffer;
}
