
#include <catch.hpp>

#include "helpers.h"

#include <libdariadb/scheme/helpers.h>
#include <libdariadb/scheme/scheme.h>
#include <libdariadb/storage/manifest.h>
#include <libdariadb/utils/fs.h>
/*
TEST_CASE("Scheme.FileTest") {
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

TEST_CASE("Scheme.Intervals") {
  const std::string storage_path = "schemeStorage";
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
  dariadb::Id raw_id, hour_median_id, hour_sigma_id, day_median_id, hour_cpu, raw_cpu;
  {
    auto settings = dariadb::storage::Settings::create(storage_path);
    auto data_scheme = dariadb::scheme::Scheme::create(settings);

    raw_id = data_scheme->addParam("memory.raw");
    raw_cpu = data_scheme->addParam("cpu.raw");
    hour_median_id = data_scheme->addParam("memory.median.hour");
    hour_sigma_id = data_scheme->addParam("memory.sigma.hour");
    hour_cpu = data_scheme->addParam("cpu.sigma.hour");
    day_median_id = data_scheme->addParam("memory.median.day");
    data_scheme->save();
  }
  {
    auto settings = dariadb::storage::Settings::create(storage_path);
    auto data_scheme = dariadb::scheme::Scheme::create(settings);

    auto all_values = data_scheme->ls();
    EXPECT_EQ(all_values.size(), size_t(6));

    auto raw_id_descr = all_values[raw_id];
    EXPECT_TRUE(raw_id_descr.aggregation_func.empty());
    EXPECT_EQ(raw_id_descr.interval, "raw");
    EXPECT_EQ(raw_id_descr.prefix(), "memory.");

    auto id_descr = all_values[hour_median_id];
    EXPECT_EQ(id_descr.name, "memory.median.hour");
    EXPECT_EQ(id_descr.aggregation_func, "median");
    EXPECT_EQ(id_descr.prefix(), "memory.median.");
    EXPECT_EQ(id_descr.interval, "hour");

    id_descr = all_values[hour_sigma_id];
    EXPECT_EQ(id_descr.name, "memory.sigma.hour");
    EXPECT_EQ(id_descr.prefix(), "memory.sigma.");
    EXPECT_EQ(id_descr.aggregation_func, "sigma");
    EXPECT_EQ(id_descr.interval, "hour");

    id_descr = all_values[day_median_id];
    EXPECT_EQ(id_descr.name, "memory.median.day");
    EXPECT_EQ(id_descr.aggregation_func, "median");
    EXPECT_EQ(id_descr.prefix(), "memory.median.");
    EXPECT_EQ(id_descr.interval, "day");

    id_descr = all_values[hour_cpu];
    EXPECT_EQ(id_descr.name, "cpu.sigma.hour");
    EXPECT_EQ(id_descr.aggregation_func, "sigma");
    EXPECT_EQ(id_descr.prefix(), "cpu.sigma.");
    EXPECT_EQ(id_descr.interval, "hour");

    auto all_hour_vaues = data_scheme->lsInterval("hour");
    EXPECT_EQ(all_hour_vaues.size(), size_t(3));

    auto linkedHours = data_scheme->linkedForValue(all_values[raw_id]);
    EXPECT_EQ(linkedHours.size(), size_t(2));
    EXPECT_TRUE(linkedHours.find(hour_sigma_id) != linkedHours.end());
    EXPECT_TRUE(linkedHours.find(hour_median_id) != linkedHours.end());

    linkedHours = data_scheme->linkedForValue(all_values[raw_cpu]);
    EXPECT_EQ(linkedHours.size(), size_t(1));
    EXPECT_TRUE(linkedHours.find(hour_cpu) != linkedHours.end());

    linkedHours = data_scheme->linkedForValue(all_values[day_median_id]);
    EXPECT_EQ(linkedHours.size(), size_t(0));

    auto median_descr = data_scheme->descriptionFor(day_median_id);
    EXPECT_EQ(median_descr.id, day_median_id);
    EXPECT_EQ(median_descr.interval, "day");

    auto unknow_descr = data_scheme->descriptionFor(77787);
    EXPECT_EQ(unknow_descr.id, dariadb::MAX_ID);
  }
  {
    auto settings = dariadb::storage::Settings::create();
    auto data_scheme = dariadb::scheme::Scheme::create(settings);

    data_scheme->addParam("param1.raw");
    auto p3_id = data_scheme->addParam("param3.raw");
    data_scheme->addParam("param3.average.minute");
    data_scheme->addParam("param33.average.minute");
    auto vm = data_scheme->ls();
    auto result = data_scheme->linkedForValue(vm[p3_id]);
    EXPECT_EQ(result.size(), size_t(1));
  }
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}*/