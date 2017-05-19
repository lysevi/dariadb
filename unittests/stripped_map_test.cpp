
#include "helpers.h"
#include <libdariadb/utils/striped_map.h>
#include <catch.hpp>
#include <thread>

using namespace dariadb::utils;

TEST_CASE("StrippedMap.OneThread") {
  {
    stripped_map<int, uint64_t> default_ctor;
    EXPECT_EQ(default_ctor.size(), size_t(0));
  }

  {
    stripped_map<int, uint64_t> add;
    add.insert(int(1), uint64_t(2));
    add.insert(int(1), uint64_t(3));
    EXPECT_EQ(add.size(), size_t(1));
    uint64_t output = 0;
    EXPECT_TRUE(add.find(int(1), &output));
    EXPECT_EQ(output, uint64_t(3));
  }
  {

    stripped_map<int, uint64_t> add;
    {
      auto iter = add.find_bucket(1);
      iter.v->second = uint64_t(3);
    }
    uint64_t output = 0;
    EXPECT_TRUE(add.find(1, &output));
    EXPECT_EQ(output, uint64_t(3));
  }

  {
    stripped_map<int, uint64_t> add_many;
    int key = 0;
    uint64_t value = 0;
    while (add_many.N() == add_many.default_n) {
      auto k = key++;
      auto v = value++;
      add_many.insert(k, v);
    }

    for (int for_search = 0; for_search < key; ++for_search) {
      uint64_t output = 0;
      if (add_many.find(for_search, &output)) {
        EXPECT_EQ(output, uint64_t(for_search));
      } else {
        INFO("key=" << for_search << " not found");
      }
    }
    size_t cnt = 0;
    auto f = [&cnt](const stripped_map<int, uint64_t>::value_type &) { cnt++; };
    add_many.apply(f);
    EXPECT_EQ(add_many.size(), size_t(key));
    EXPECT_EQ(add_many.size(), cnt);
  }
}

void StrippedMap_write_thread_func(stripped_map<int, int> *target, int key_from,
                                   int key_to) {
  for (auto i = key_from; i < key_to; ++i) {
    target->insert(i, i);
  }
}

TEST_CASE("StrippedMap.MultiThread") {
  stripped_map<int, int> smap;
  const size_t count = 10;
  std::vector<std::thread> wthreads(count);
  for (size_t i = 0; i < count; ++i) {
    wthreads[i] =
        std::thread(StrippedMap_write_thread_func, &smap, i * 10, i * 10 + count);
  }

  for (size_t i = 0; i < count; ++i) {
    wthreads[i].join();
  }

  size_t cnt = 0;
  auto f = [&cnt](const stripped_map<int, int>::value_type &) { cnt++; };
  smap.apply(f);
  EXPECT_EQ(cnt, count * count);
}