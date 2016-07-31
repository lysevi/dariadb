#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <algorithm>
#include <boost/test/unit_test.hpp>

#include <compression/compression.h>
#include <flags.h>
#include <storage/bloom_filter.h>
#include <storage/callbacks.h>
#include <storage/chunk.h>
#include <storage/manifest.h>
#include <storage/page.h>
#include <storage/page_manager.h>
#include <storage/options.h>
#include <utils/fs.h>
#include <utils/thread_manager.h>
#include <utils/utils.h>

using dariadb::storage::PageManager;
using dariadb::storage::Manifest;

BOOST_AUTO_TEST_CASE(ManifestFileTest) {
  const std::string fname = "manifest";
  if (dariadb::utils::fs::path_exists(fname)) {
    dariadb::utils::fs::rm(fname);
  }

  {
    Manifest::start(fname);
    std::list<std::string> pages_names{"1", "2", "3"};
    for (auto n : pages_names) {
      Manifest::instance()->page_append(n);
    }

    std::list<std::string> cola_names{"11", "22", "33"};
    for (auto n : cola_names) {
      Manifest::instance()->cola_append(n);
    }

    std::list<std::string> aof_names{"111", "222", "333"};
    for (auto n : aof_names) {
      Manifest::instance()->aof_append(n);
    }

    auto page_lst = Manifest::instance()->page_list();
    BOOST_CHECK_EQUAL(page_lst.size(), pages_names.size());
    BOOST_CHECK_EQUAL_COLLECTIONS(page_lst.begin(), page_lst.end(), pages_names.begin(),
                                  pages_names.end());

    auto cola_lst = Manifest::instance()->cola_list();
    BOOST_CHECK_EQUAL(cola_lst.size(), cola_names.size());
    BOOST_CHECK_EQUAL_COLLECTIONS(cola_lst.begin(), cola_lst.end(), cola_names.begin(),
                                  cola_names.end());

    auto aof_lst = Manifest::instance()->aof_list();
    BOOST_CHECK_EQUAL(aof_lst.size(), aof_names.size());
    BOOST_CHECK_EQUAL_COLLECTIONS(aof_lst.begin(), aof_lst.end(), aof_names.begin(),
                                  aof_names.end());

    Manifest::stop();
  }
  { // reopen. restore method must remove all records from manifest.
    Manifest::start(fname);
    BOOST_CHECK_EQUAL(Manifest::instance()->page_list().size(), size_t(0));
    BOOST_CHECK_EQUAL(Manifest::instance()->cola_list().size(), size_t(0));
    BOOST_CHECK_EQUAL(Manifest::instance()->aof_list().size(), size_t(0));
    Manifest::stop();
  }
  if (dariadb::utils::fs::path_exists(fname)) {
    dariadb::utils::fs::rm(fname);
  }
}

BOOST_AUTO_TEST_CASE(ChunkSorted) {
  uint8_t buffer[1024];
  std::fill(std::begin(buffer), std::end(buffer), 0);
  dariadb::storage::ChunkHeader info;
  auto v = dariadb::Meas::empty(0);
  v.time = 5;
  dariadb::storage::Chunk_Ptr zch{
      new dariadb::storage::ZippedChunk(&info, buffer, 1024, v)};
  BOOST_CHECK(zch->header->is_sorted);
  v.time++;
  BOOST_CHECK(zch->append(v));
  v.time++;
  BOOST_CHECK(zch->append(v));
  v.time = 4;
  BOOST_CHECK(zch->append(v));
  BOOST_CHECK(!zch->header->is_sorted);
  v.time = 10;
  BOOST_CHECK(zch->append(v));
  BOOST_CHECK(!zch->header->is_sorted);

  auto reader = zch->get_reader();
  auto prev_value = reader->readNext();
  while (!reader->is_end()) {
    auto new_value = reader->readNext();
    BOOST_CHECK_LE(prev_value.time, new_value.time);
    prev_value = new_value;
  }
}

BOOST_AUTO_TEST_CASE(PageManagerInstance) {
  const std::string storagePath = "testStorage";
  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
  dariadb::utils::async::ThreadManager::start(
      dariadb::utils::async::THREAD_MANAGER_COMMON_PARAMS);
  dariadb::storage::Manifest::start(
      dariadb::utils::fs::append_path(storagePath, dariadb::storage::MANIFEST_FILE_NAME));
  dariadb::storage::Options::start();
  dariadb::storage::Options::instance()->path=storagePath;
  dariadb::storage::Options::instance()->page_chunk_size=1;
  PageManager::start();

  BOOST_CHECK(PageManager::instance() != nullptr);
  PageManager::stop();
  dariadb::storage::Manifest::stop();
  dariadb::utils::async::ThreadManager::stop();
  dariadb::storage::Options::stop();
  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
}


BOOST_AUTO_TEST_CASE(PageManagerReadWriteWithContinue) {
  const std::string storagePath = "testStorage";
  const size_t chunks_size = 200;
  auto t = dariadb::Time(0);

  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
  dariadb::Meas::MeasList addeded;
  dariadb::utils::async::ThreadManager::start(
      dariadb::utils::async::THREAD_MANAGER_COMMON_PARAMS);
  dariadb::storage::Manifest::start(
      dariadb::utils::fs::append_path(storagePath, dariadb::storage::MANIFEST_FILE_NAME));

  dariadb::storage::Options::start();
  dariadb::storage::Options::instance()->path=storagePath;
  dariadb::storage::Options::instance()->page_chunk_size=chunks_size;

  PageManager::start();
  dariadb::Meas first;
  first.id = 1;
  first.time = t;
  {
	  dariadb::Meas first;
	  first.id = 1;
	  first.time = t;
	  auto count = chunks_size / 10;
	  dariadb::Meas::MeasArray ma;
	  ma.resize(count);
	  for (size_t i = 0; i < count; i++, t++) {
		  first.flag = dariadb::Flag(i);
		  first.time = t;
		  first.value = dariadb::Value(i);
		  ma[i] = first;
		  addeded.push_back(first);
	  }
	  PageManager::instance()->append("pagename", ma);
  }
  PageManager::stop();

  auto fname = dariadb::utils::fs::ls(storagePath, dariadb::storage::PAGE_FILE_EXT).front();
  auto header = dariadb::storage::Page::readHeader(fname);
  BOOST_CHECK(header.addeded_chunks!=size_t(0));

  auto iheader = dariadb::storage::Page::readIndexHeader(fname + "i");
  BOOST_CHECK(iheader.count != 0);

  PageManager::start();

  auto mintime_chunks =
      PageManager::instance()->valuesBeforeTimePoint(dariadb::storage::QueryTimePoint(
          dariadb::IdArray{1}, 0, PageManager::instance()->minTime()));
  BOOST_CHECK_GE(mintime_chunks.size(), size_t(1));

  PageManager::stop();
  dariadb::storage::Manifest::stop();
  dariadb::utils::async::ThreadManager::stop();
  dariadb::storage::Options::stop();
  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
}

BOOST_AUTO_TEST_CASE(PageManagerMultiPageRead) {
  const std::string storagePath = "testStorage";
  const size_t chunks_size = 200;
  auto t = dariadb::Time(0);

  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
  dariadb::Meas::MeasList addeded;
  dariadb::utils::async::ThreadManager::start(
      dariadb::utils::async::THREAD_MANAGER_COMMON_PARAMS);
  dariadb::storage::Manifest::start(
      dariadb::utils::fs::append_path(storagePath, dariadb::storage::MANIFEST_FILE_NAME));

  dariadb::storage::Options::start();
  dariadb::storage::Options::instance()->path=storagePath;
  dariadb::storage::Options::instance()->page_chunk_size=chunks_size;

  PageManager::start();
  dariadb::Meas first;
  first.id = 1;
  first.time = t;
  const size_t page_count = 4;

  size_t iteration = 0;
  while (dariadb::utils::fs::ls(storagePath, dariadb::storage::PAGE_FILE_EXT).size() <= page_count) {
	dariadb::Meas first;
	first.id = 1;
	first.time = t;
	auto count = chunks_size / 10;
	dariadb::Meas::MeasArray ma;
	ma.resize(count);
	for (size_t i = 0; i < count; i++, t++) {
		first.flag = dariadb::Flag(i);
		first.time = t;
		first.value = dariadb::Value(i);
		ma[i] = first;
		addeded.push_back(first);
	}
	std::stringstream ss;
	ss << std::string("pagename") << iteration++;
	PageManager::instance()->append(ss.str(), ma);
  }

  dariadb::storage::QueryInterval qi(dariadb::IdArray{1}, 0, addeded.front().time,
                                     addeded.back().time);

  dariadb::storage::QueryTimePoint qt(
      dariadb::IdArray{1}, 0,
      addeded.front().time + (addeded.back().time - addeded.front().time) / 2);

  auto link_list = PageManager::instance()->chunksByIterval(qi);

  auto clb = std::unique_ptr<dariadb::storage::MList_ReaderClb>{
      new dariadb::storage::MList_ReaderClb};
  PageManager::instance()->readLinks(qi, link_list, clb.get());

  size_t writed = addeded.size();
  size_t readed = clb->mlist.size();

  BOOST_CHECK_EQUAL(readed, writed);

  auto id2meas = PageManager::instance()->valuesBeforeTimePoint(qt);

  BOOST_CHECK_EQUAL(id2meas.size(), qt.ids.size());
  for (auto kv : id2meas) {
    BOOST_CHECK_LE(kv.second.time, qt.time_point);
  }

  dariadb::Time minTime, maxTime;
  if (PageManager::instance()->minMaxTime(1, &minTime, &maxTime)) {
    BOOST_CHECK_EQUAL(minTime, qi.from);
    BOOST_CHECK_EQUAL(maxTime, qi.to);
  } else {
    BOOST_ERROR("PageManager::instance()->minMaxTime error!");
  }

  PageManager::stop();
  dariadb::storage::Manifest::stop();
  dariadb::utils::async::ThreadManager::stop();
  dariadb::storage::Options::stop();
  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
}


BOOST_AUTO_TEST_CASE(PageManagerBulkWrite) {
  const std::string storagePath = "testStorage";
  const size_t chunks_size = 256;

  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
  dariadb::utils::async::ThreadManager::start(
      dariadb::utils::async::THREAD_MANAGER_COMMON_PARAMS);
  dariadb::storage::Manifest::start(
      dariadb::utils::fs::append_path(storagePath, dariadb::storage::MANIFEST_FILE_NAME));

  dariadb::storage::Options::start();
  dariadb::storage::Options::instance()->path=storagePath;
  dariadb::storage::Options::instance()->page_chunk_size=chunks_size;

  PageManager::start();
  BOOST_CHECK(PageManager::instance() != nullptr);

  auto start_time = dariadb::Time(0);
  dariadb::Meas::MeasList addeded;
  const dariadb::Id id_count(5);
  dariadb::IdSet all_id_set;
  size_t count = 5000;
  dariadb::Meas::MeasArray a(count);
  auto e=dariadb::Meas::empty();
  for (size_t i = 0; i < count; i++) {
    e.id = i % id_count;
    e.time++;
    e.value = dariadb::Value(i);
    a[i] = e;
    all_id_set.insert(e.id);
    addeded.push_back(e);
  }
  std::string page_file_prefix="page_prefix";
  PageManager::instance()->append(page_file_prefix,a);

  dariadb::IdArray all_id_array{all_id_set.begin(), all_id_set.end()};
  { // Chunks load
    // must return all of appended chunks;

    PageManager::instance()->flush();
    dariadb::storage::QueryInterval qi(all_id_array, 0, 0, e.time);
    auto links_list = PageManager::instance()->chunksByIterval(qi);

    auto clb = std::unique_ptr<dariadb::storage::MList_ReaderClb>{
        new dariadb::storage::MList_ReaderClb};

    PageManager::instance()->readLinks(qi, links_list, clb.get());

    BOOST_CHECK_EQUAL(addeded.size(), clb->mlist.size());
    dariadb::Time minT = dariadb::MAX_TIME, maxT = dariadb::MIN_TIME;
    BOOST_CHECK(PageManager::instance()->minMaxTime(dariadb::Id(0), &minT, &maxT));
    BOOST_CHECK_EQUAL(minT, dariadb::Time(1));
    BOOST_CHECK_GE(maxT, minT);
    {
      dariadb::Time end_time(e.time / 2);
      dariadb::storage::ChunksList chunk_list;
      dariadb::storage::QueryInterval qi(all_id_array, 0, start_time, end_time);
      auto link_list = PageManager::instance()->chunksByIterval(qi);

      auto clb = std::unique_ptr<dariadb::storage::MList_ReaderClb>{
          new dariadb::storage::MList_ReaderClb};

      PageManager::instance()->readLinks(qi, links_list, clb.get());

      BOOST_CHECK_GT(clb->mlist.size(), size_t(0));

      for (auto &m : clb->mlist) {
        BOOST_CHECK(m.time <= end_time);
      }

      auto id2meas_res = PageManager::instance()->valuesBeforeTimePoint(
          dariadb::storage::QueryTimePoint(all_id_array, 0, end_time));
      BOOST_CHECK_EQUAL(id2meas_res.size(), all_id_array.size());

      for (auto &kv : id2meas_res) {
        BOOST_CHECK(kv.second.time <= end_time);
      }
    }
  }
  BOOST_CHECK(dariadb::utils::fs::path_exists(storagePath));
  BOOST_CHECK(dariadb::utils::fs::ls(storagePath).size() == 3); // page +index+manifest

  PageManager::stop();
  dariadb::storage::Manifest::stop();
  dariadb::utils::async::ThreadManager::stop();
  dariadb::storage::Options::stop();
  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
}
