#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include "test_common.h"
#include <boost/test/unit_test.hpp>

#include <libdariadb/scheme/scheme.h>
#include <libdariadb/scheme/helpers.h>
#include <libdariadb/storage/manifest.h>
#include <libdariadb/utils/fs.h>

BOOST_AUTO_TEST_CASE(ManifestFileTest) {
  const std::string storage_path = "SchemeStorage";
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }

  std::string version = "0.1.2.3.4.5";

  {
    auto settings = dariadb::storage::Settings::create(storage_path);
    auto manifest = dariadb::storage::Manifest::create(settings);
    manifest = nullptr;
  }
 
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}
