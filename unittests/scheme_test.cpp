#include <gtest/gtest.h>

#include "helpers.h"

#include <libdariadb/scheme/helpers.h>
#include <libdariadb/scheme/scheme.h>
#include <libdariadb/storage/manifest.h>
#include <libdariadb/utils/fs.h>

TEST(Scheme, FileTest) {
  const std::string storage_path = "schemeStorage";
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }

  dariadb::Id i1, i2, i3, i4, i5, i6;

  {
    auto settings = dariadb::storage::Settings::create(storage_path);
    auto data_scheme = dariadb::scheme::Scheme::create(settings);

    i1 = data_scheme->addParam("lvl1.lvl2.lvl3_1.param1");
    auto i1_2 = data_scheme->addParam("lvl1.lvl2.lvl3_1.param1");
    EXPECT_EQ(i1, i1_2);
    i2 = data_scheme->addParam("lvl1.lvl2.lvl3_1.param2");
    i3 = data_scheme->addParam("lvl1.lvl2.lvl3_1.param3");

    i4 = data_scheme->addParam("lvl1.lvl2.lvl3_2.param1");
    i5 = data_scheme->addParam("lvl1.lvl2.param1");
    i6 = data_scheme->addParam("lvl1.lvl2.lvl3.lvl4.param1");

    EXPECT_TRUE(i1 != i2 && i2 != i3 && i3 != i4 && i4 != i5 && i5 != i6);

    data_scheme->save();
    auto scheme_files = dariadb::utils::fs::ls(storage_path, ".js");
    EXPECT_EQ(scheme_files.size(), size_t(1));
  }
  {
    auto settings = dariadb::storage::Settings::create(storage_path);
    auto data_scheme = dariadb::scheme::Scheme::create(settings);
    auto all_values = data_scheme->ls();
    EXPECT_EQ(all_values.size(), size_t(6));

    EXPECT_TRUE(all_values.idByParam("lvl1.lvl2.lvl3_1.param1") == i1);

    for (auto kv : all_values) {
      auto md = kv.second;
      if (md.name == "lvl1.lvl2.lvl3_1.param1") {
        EXPECT_EQ(md.id, i1);
      }
      if (md.name == "lvl1.lvl2.lvl3_1.param2") {
        EXPECT_EQ(md.id, i2);
      }
      if (md.name == "lvl1.lvl2.lvl3_1.param3") {
        EXPECT_EQ(md.id, i3);
      }
      if (md.name == "lvl1.lvl2.lvl3_2.param1") {
        EXPECT_EQ(md.id, i4);
      }
      if (md.name == "lvl1.lvl2.param1") {
        EXPECT_EQ(md.id, i5);
      }
      if (md.name == "lvl1.lvl2.lvl3.lvl4.param1") {
        EXPECT_EQ(md.id, i6);
      }
    }

    auto i7 = data_scheme->addParam("lvl1.lvl2.lvl3.lvl4.param77");
    EXPECT_TRUE(i6 < i7);
    EXPECT_TRUE(i7 != dariadb::Id(0));
  }

  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

TEST(Scheme, Intervals) {
  const std::string storage_path = "schemeStorage";
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
  dariadb::Id raw_id, hour_median_id, hour_sigma_id, day_median_id;
  {
    auto settings = dariadb::storage::Settings::create(storage_path);
    auto data_scheme = dariadb::scheme::Scheme::create(settings);

    raw_id = data_scheme->addParam("memory.raw");
    hour_median_id = data_scheme->addParam("memory.median.hour");
    hour_sigma_id = data_scheme->addParam("memory.sigma.hour");
    day_median_id = data_scheme->addParam("memory.median.day");
    data_scheme->save();
  }
  {
    auto settings = dariadb::storage::Settings::create(storage_path);
    auto data_scheme = dariadb::scheme::Scheme::create(settings);

    auto all_values = data_scheme->ls();
    
	auto raw_id_descr = all_values[raw_id];
    EXPECT_TRUE(raw_id_descr.aggregation_func.empty());
    EXPECT_EQ(raw_id_descr.interval, "raw");

	auto id_descr = all_values[hour_median_id];
	EXPECT_EQ(id_descr.name, "memory.median.hour");
	EXPECT_EQ(id_descr.aggregation_func,"median");
	EXPECT_EQ(id_descr.interval, "hour");


	id_descr = all_values[hour_sigma_id];
	EXPECT_EQ(id_descr.name, "memory.sigma.hour");
	EXPECT_EQ(id_descr.aggregation_func, "sigma");
	EXPECT_EQ(id_descr.interval, "hour");

	id_descr = all_values[day_median_id];
	EXPECT_EQ(id_descr.name, "memory.median.day");
	EXPECT_EQ(id_descr.aggregation_func, "median");
	EXPECT_EQ(id_descr.interval, "day");
  }
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}