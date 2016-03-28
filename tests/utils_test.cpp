#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <boost/test/unit_test.hpp>
#include <utils.h>
#include <storage/fs.h>

BOOST_AUTO_TEST_CASE(InInterval) {
  BOOST_CHECK(dariadb::utils::inInterval(1, 5, 1));
  BOOST_CHECK(dariadb::utils::inInterval(1, 5, 2));
  BOOST_CHECK(dariadb::utils::inInterval(1, 5, 5));
  BOOST_CHECK(!dariadb::utils::inInterval(1, 5, 0));
  BOOST_CHECK(!dariadb::utils::inInterval(0, 1, 2));
}

BOOST_AUTO_TEST_CASE(BitOperations) {
    uint8_t value = 0;
	for (int8_t i = 0; i < 7; i++) {
        value=dariadb::utils::BitOperations::set(value, i);
		BOOST_CHECK_EQUAL(dariadb::utils::BitOperations::check(value, i), true);
	}

	for (int8_t i = 0; i < 7; i++) {
        value=dariadb::utils::BitOperations::clr(value, i);
	}

	for (int8_t i = 0; i < 7; i++) {
		BOOST_CHECK_EQUAL(dariadb::utils::BitOperations::check(value, i), false);
	}
}

BOOST_AUTO_TEST_CASE(FileUtils) {
  std::string filename = "foo/bar/test.txt";
  BOOST_CHECK_EQUAL(dariadb::utils::fs::filename(filename), "test");
  BOOST_CHECK_EQUAL(dariadb::utils::fs::parent_path(filename), "foo/bar");

  auto ls_res=dariadb::utils::fs::ls(".");
  BOOST_CHECK(ls_res.size()>0);

  const std::string fname="mapped_file.test";
  auto mapf=dariadb::utils::fs::MappedFile::touch(fname,1024);
  for(uint8_t i=0;i<100;i++){
      mapf->data()[i]=i;
  }
  mapf->close();

  ls_res=dariadb::utils::fs::ls(".",".test");
  BOOST_CHECK(ls_res.size()==1);
  auto reopen_mapf=dariadb::utils::fs::MappedFile::open(fname);
  for(uint8_t i=0;i<100;i++){
      BOOST_CHECK_EQUAL(reopen_mapf->data()[i],i);
  }
  reopen_mapf->close();
  dariadb::utils::fs::rm(fname);

  std::string parent_p = "path1";
  std::string child_p = "path2";
  auto concat_p=dariadb::utils::fs::append_path(parent_p, child_p);
  BOOST_CHECK_EQUAL(dariadb::utils::fs::parent_path(concat_p), parent_p);
}
