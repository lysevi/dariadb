#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include "test_common.h"
#include <boost/test/unit_test.hpp>

#include <libdariadb/scheme/helpers.h>
#include <libdariadb/scheme/scheme.h>
#include <libdariadb/storage/manifest.h>
#include <libdariadb/utils/fs.h>

BOOST_AUTO_TEST_CASE(SchemeFileTest) {
  const std::string storage_path = "schemeStorage";
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }

  {
    auto settings = dariadb::storage::Settings::create(storage_path);
    auto data_scheme = dariadb::scheme::Scheme::create(settings);

    data_scheme->addParam("lvl1.lvl2.lvl3_1.param1");
    data_scheme->addParam("lvl1.lvl2.lvl3_1.param2");
    data_scheme->addParam("lvl1.lvl2.lvl3_1.param3");

    data_scheme->addParam("lvl1.lvl2.lvl3_2.param1");
    data_scheme->addParam("lvl1.lvl2.param1");
    data_scheme->addParam("lvl1.lvl2.lvl3.lvl4.param1");

    data_scheme->save();
    auto scheme_files = dariadb::utils::fs::ls(storage_path, ".js");
    BOOST_CHECK_EQUAL(scheme_files.size(), size_t(1));
  }
  {
    auto settings = dariadb::storage::Settings::create(storage_path);
    auto data_scheme = dariadb::scheme::Scheme::create(settings);
    auto all_values = data_scheme->ls();
    BOOST_CHECK_EQUAL(all_values.size(), size_t(6));
  }

  /* if (dariadb::utils::fs::path_exists(storage_path)) {
     dariadb::utils::fs::rm(storage_path);
   }*/
}
