#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include "test_common.h"
#include <boost/test/unit_test.hpp>

#include <libdariadb/interfaces/icursor.h>
#include <libdariadb/storage/bloom_filter.h>
#include <libdariadb/storage/chunk.h>
#include <libdariadb/storage/cursors.h>
#include <libdariadb/storage/manifest.h>
#include <libdariadb/utils/fs.h>

#include <iostream>

BOOST_AUTO_TEST_CASE(MeasTest) {
  dariadb::Meas m;
  m.flag = dariadb::Flag(1);
  BOOST_CHECK(!m.inFlag(dariadb::Flag(2)));
  m.flag = 3;
  BOOST_CHECK(m.inFlag(dariadb::Flag(1)));
}

BOOST_AUTO_TEST_CASE(BloomTest) {
  uint64_t u8_fltr = dariadb::storage::bloom_empty<uint8_t>();

  BOOST_CHECK_EQUAL(u8_fltr, uint64_t{0});

  u8_fltr = dariadb::storage::bloom_add(u8_fltr, uint8_t{1});
  u8_fltr = dariadb::storage::bloom_add(u8_fltr, uint8_t{2});
  u8_fltr = dariadb::storage::bloom_add(u8_fltr, uint8_t{3});

  BOOST_CHECK(dariadb::storage::bloom_check(u8_fltr, uint8_t{1}));
  BOOST_CHECK(dariadb::storage::bloom_check(u8_fltr, uint8_t{2}));
  BOOST_CHECK(dariadb::storage::bloom_check(u8_fltr, uint8_t{3}));

  uint64_t u8_fltr_2 = dariadb::storage::bloom_empty<uint8_t>();
  u8_fltr_2 = dariadb::storage::bloom_add(u8_fltr_2, uint8_t{4});

  BOOST_CHECK(!dariadb::storage::bloom_check(u8_fltr, uint8_t{4}));
  BOOST_CHECK(dariadb::storage::bloom_check(u8_fltr_2, uint8_t{4}));

  auto super_fltr = dariadb::storage::bloom_combine(u8_fltr, u8_fltr_2);
  BOOST_CHECK(dariadb::storage::bloom_check(super_fltr, uint8_t{1}));
  BOOST_CHECK(dariadb::storage::bloom_check(super_fltr, uint8_t{2}));
  BOOST_CHECK(dariadb::storage::bloom_check(super_fltr, uint8_t{3}));
  BOOST_CHECK(dariadb::storage::bloom_check(super_fltr, uint8_t{4}));
}

BOOST_AUTO_TEST_CASE(inFilter) {
  {
    auto m = dariadb::Meas();
    m.flag = 100;
    BOOST_CHECK(m.inFlag(0));
    BOOST_CHECK(m.inFlag(100));
    BOOST_CHECK(!m.inFlag(10));
  }
}

BOOST_AUTO_TEST_CASE(StatisticUpdate) {
  dariadb::Statistic st;

  BOOST_CHECK_EQUAL(st.minTime, dariadb::MAX_TIME);
  BOOST_CHECK_EQUAL(st.maxTime, dariadb::MIN_TIME);
  BOOST_CHECK_EQUAL(st.count, uint32_t(0));
  BOOST_CHECK_EQUAL(st.flag_bloom, dariadb::Flag(0));
  BOOST_CHECK_EQUAL(st.minValue, dariadb::MAX_VALUE);
  BOOST_CHECK_EQUAL(st.maxValue, dariadb::MIN_VALUE);
  BOOST_CHECK_EQUAL(st.sum, dariadb::Value(0));

  auto m = dariadb::Meas(0);
  m.time = 2;
  m.flag = 2;
  m.value = 2;
  st.update(m);
  BOOST_CHECK_EQUAL(st.minTime, m.time);
  BOOST_CHECK_EQUAL(st.maxTime, m.time);
  BOOST_CHECK(st.flag_bloom != dariadb::Flag(0));
  BOOST_CHECK(dariadb::areSame(st.minValue, m.value));
  BOOST_CHECK(dariadb::areSame(st.maxValue, m.value));

  m.time = 3;
  m.value = 3;
  st.update(m);
  BOOST_CHECK_EQUAL(st.minTime, dariadb::Time(2));
  BOOST_CHECK_EQUAL(st.maxTime, dariadb::Time(3));
  BOOST_CHECK(dariadb::areSame(st.minValue, dariadb::Value(2)));
  BOOST_CHECK(dariadb::areSame(st.maxValue, dariadb::Value(3)));

  m.time = 1;
  m.value = 1;
  st.update(m);
  BOOST_CHECK_EQUAL(st.minTime, dariadb::Time(1));
  BOOST_CHECK_EQUAL(st.maxTime, dariadb::Time(3));
  BOOST_CHECK(dariadb::areSame(st.minValue, dariadb::Value(1)));
  BOOST_CHECK(dariadb::areSame(st.maxValue, dariadb::Value(3)));
  BOOST_CHECK(dariadb::areSame(st.sum, dariadb::Value(6)));
  BOOST_CHECK_EQUAL(st.count, uint32_t(3));

  dariadb::Statistic second_st;
  m.time = 777;
  m.value = 1;
  second_st.update(m);
  BOOST_CHECK_EQUAL(second_st.maxTime, m.time);

  second_st.update(st);
  BOOST_CHECK_EQUAL(second_st.minTime, dariadb::Time(1));
  BOOST_CHECK_EQUAL(second_st.maxTime, dariadb::Time(777));
  BOOST_CHECK(dariadb::areSame(second_st.minValue, dariadb::Value(1)));
  BOOST_CHECK(dariadb::areSame(second_st.maxValue, dariadb::Value(3)));
  BOOST_CHECK(dariadb::areSame(second_st.sum, dariadb::Value(7)));
  BOOST_CHECK_EQUAL(second_st.count, uint32_t(4));
}

BOOST_AUTO_TEST_CASE(ManifestFileTest) {
  const std::string storage_path = "emptyStorage";
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }

  std::string version = "0.1.2.3.4.5";

  {
    auto settings = dariadb::storage::Settings::create(storage_path);
    auto manifest = dariadb::storage::Manifest::create(settings);
    std::list<std::string> pages_names{"1", "2", "3"};
    for (auto n : pages_names) {
      manifest->page_append(n);
    }

    manifest->set_format(version);

    std::list<std::string> wal_names{"111", "222", "333"};
    for (auto n : wal_names) {
      manifest->wal_append(n);
    }

    auto page_lst = manifest->page_list();
    BOOST_CHECK_EQUAL(page_lst.size(), pages_names.size());
    BOOST_CHECK_EQUAL_COLLECTIONS(page_lst.begin(), page_lst.end(),
                                  pages_names.begin(), pages_names.end());

    auto wal_lst = manifest->wal_list();
    BOOST_CHECK_EQUAL(wal_lst.size(), wal_names.size());
    BOOST_CHECK_EQUAL_COLLECTIONS(wal_lst.begin(), wal_lst.end(),
                                  wal_names.begin(), wal_names.end());

    manifest = nullptr;
  }
  { // reopen. restore method must remove all records from manifest.
    auto settings = dariadb::storage::Settings::create(storage_path);
    auto manifest = dariadb::storage::Manifest::create(settings);
    BOOST_CHECK_EQUAL(manifest->page_list().size(), size_t(0));
    BOOST_CHECK_EQUAL(manifest->wal_list().size(), size_t(0));
    BOOST_CHECK_EQUAL(manifest->get_format(), version);
  }

  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

BOOST_AUTO_TEST_CASE(SettingsInstance) {
  const std::string storage_path = "testStorage";
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }

  dariadb::utils::fs::mkdir(storage_path);

  auto settings = dariadb::storage::Settings::create(storage_path);

  settings->wal_cache_size.setValue(2);
  settings->chunk_size.setValue(7);
  settings->strategy.setValue(dariadb::STRATEGY::COMPRESSED);
  settings->max_pages_in_level.setValue(10);
  settings->save();

  settings = nullptr;

  bool file_exists =
      dariadb::utils::fs::path_exists(dariadb::utils::fs::append_path(
          storage_path, dariadb::storage::SETTINGS_FILE_NAME));
  BOOST_CHECK(file_exists);

  settings = dariadb::storage::Settings::create(storage_path);
  BOOST_CHECK_EQUAL(settings->wal_cache_size.value(), uint64_t(2));
  BOOST_CHECK_EQUAL(settings->chunk_size.value(), uint32_t(7));
  BOOST_CHECK_EQUAL(settings->max_pages_in_level.value(), uint16_t(10));
  BOOST_CHECK(settings->strategy.value() == dariadb::STRATEGY::COMPRESSED);

  settings = nullptr;
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

BOOST_AUTO_TEST_CASE(ChunkTest) {
  {
    dariadb::storage::ChunkHeader hdr;
    uint8_t *buff = new uint8_t[1024];
    std::fill_n(buff, 1024, uint8_t(0));
    auto m = dariadb::Meas();
    auto ch = dariadb::storage::Chunk::create(&hdr, buff, 1024, m);
    m.time = 0;
    while (!ch->isFull()) {
      ch->append(m);
      m.time++;
    }
    BOOST_CHECK_EQUAL(hdr.is_sorted, uint8_t(1));
    dariadb_test::check_reader(ch->getReader());
    delete[] buff;
  }
  {
    size_t writed = 1;
    dariadb::storage::ChunkHeader hdr;
    uint8_t *buff = new uint8_t[1024];
    std::fill_n(buff, 1024, uint8_t(0));
    auto m = dariadb::Meas();
    m.time = 999;
    auto ch = dariadb::storage::Chunk::create(&hdr, buff, 1024, m);
    m.time = 0;
    while (!ch->isFull()) {
      if (ch->append(m)) {
        writed++;
      }
      m.time++;
    }
    BOOST_CHECK_EQUAL(hdr.is_sorted, uint8_t(0));
    dariadb_test::check_reader(ch->getReader());
    {
      auto skip_size = dariadb::storage::Chunk::compact(&hdr);
      BOOST_CHECK(dariadb::storage::Chunk::compact(&hdr) == uint32_t(0));
      auto ch2 = dariadb::storage::Chunk::open(&hdr, buff + skip_size);
      auto rdr2 = ch2->getReader();
      size_t readed = 0;
      while (!rdr2->is_end()) {
        rdr2->readNext();
        readed++;
      }
      BOOST_CHECK_EQUAL(readed, writed);
    }

    delete[] buff;
  }
}

BOOST_AUTO_TEST_CASE(LinearReaderTest) {
  using namespace dariadb::storage;
  using namespace dariadb;
  dariadb::MeasArray ma1(4);
  ma1[0].time = 1;
  ma1[1].time = 2;
  ma1[2].time = 3;
  ma1[3].time = 4;
  auto fr1 = dariadb::Cursor_Ptr{new FullCursor(ma1)};

  dariadb::MeasArray ma2(4);
  ma2[0].time = 5;
  ma2[1].time = 6;
  ma2[2].time = 7;
  ma2[3].time = 8;
  auto fr2 = dariadb::Cursor_Ptr{new FullCursor(ma2)};

  dariadb::storage::LinearCursor lr(CursorsList{fr1, fr2});

  dariadb::MeasList ml;
  while (!lr.is_end()) {
    auto v = lr.readNext();
    ml.push_back(v);

    if (!lr.is_end()) {
      auto tp = lr.top();
      BOOST_CHECK_LT(v.time, tp.time);
    }
  }

  BOOST_CHECK_EQUAL(ml.size(), size_t(8));
}

BOOST_AUTO_TEST_CASE(MergeSortReaderTest) {
  using namespace dariadb::storage;
  using namespace dariadb;
  dariadb::MeasArray ma1(4);
  ma1[0].time = 1;
  ma1[1].time = 2;
  ma1[2].time = 4;
  ma1[3].time = 7;
  auto fr1 = dariadb::Cursor_Ptr{new FullCursor(ma1)};

  dariadb::MeasArray ma2(4);
  ma2[0].time = 3;
  ma2[1].time = 5;
  ma2[2].time = 6;
  ma2[3].time = 7;
  auto fr2 = dariadb::Cursor_Ptr{new FullCursor(ma2)};

  dariadb::MeasArray ma3(1);
  ma3[0].time = 8;
  auto fr3 = dariadb::Cursor_Ptr{new FullCursor(ma3)};

  dariadb::storage::MergeSortCursor msr{CursorsList{fr1, fr2, fr3}};

  dariadb::MeasList ml;
  while (!msr.is_end()) {
    auto v = msr.readNext();
    ml.push_back(v);

    if (!msr.is_end()) {
      auto tp = msr.top();
      BOOST_CHECK_LT(v.time, tp.time);
    }
  }

  auto must_be_false =
      dariadb::storage::CursorWrapperFactory::is_linear_readers(fr1, fr2);
  auto must_be_true =
      dariadb::storage::CursorWrapperFactory::is_linear_readers(fr1, fr3);

  BOOST_CHECK(must_be_true);
  BOOST_CHECK(!must_be_false);

  BOOST_CHECK_EQUAL(ml.size(), size_t(8));

  for (auto it = ml.begin(); it != ml.end(); ++it) {
    auto cur_value = *it;
    auto next = std::next(it);
    if (next == ml.end()) {
      break;
    }
    auto next_value = *next;

    BOOST_CHECK_LT(cur_value.time, next_value.time);
  }
}

BOOST_AUTO_TEST_CASE(ReaderColapseTest) {
  using namespace dariadb::storage;
  using namespace dariadb;
  MeasArray ma1(4);
  ma1[0].time = 1;
  ma1[1].time = 2;
  ma1[2].time = 4;
  ma1[3].time = 7;
  auto fr1 = Cursor_Ptr{new FullCursor(ma1)};

  MeasArray ma2(4);
  ma2[0].time = 3;
  ma2[1].time = 5;
  ma2[2].time = 6;
  ma2[3].time = 7;
  auto fr2 = Cursor_Ptr{new FullCursor(ma2)};

  MeasArray ma3(1);
  ma3[0].time = 8;
  auto fr3 = Cursor_Ptr{new FullCursor(ma3)};

  {
    auto msr = CursorWrapperFactory::colapseCursors(CursorsList{fr1, fr2});
    auto top_reader = dynamic_cast<LinearCursor *>(msr.get());
    auto is_merge_reader = dynamic_cast<MergeSortCursor *>(
                               top_reader->_readers.front().get()) != nullptr;
    BOOST_CHECK(is_merge_reader);
    BOOST_CHECK_EQUAL(top_reader->_readers.size(), size_t(1));
  }

  {
    auto lsr = CursorWrapperFactory::colapseCursors(CursorsList{fr1, fr3});
    auto top_reader = dynamic_cast<LinearCursor *>(lsr.get());
    for (auto &r : top_reader->_readers) {
      auto is_full_reader = dynamic_cast<FullCursor *>(r.get()) != nullptr;
      BOOST_CHECK(is_full_reader);
    }
  }
}

BOOST_AUTO_TEST_CASE(JoinTest) {
  using namespace dariadb::storage;
  using namespace dariadb;
  MeasArray ma1(5);
  ma1[0].time = 1;
  ma1[1].time = 2;
  ma1[2].time = 3;
  ma1[3].time = 4;
  ma1[4].time = 5;
  ma1[0].flag = ma1[1].flag = ma1[2].flag = ma1[3].flag = ma1[4].flag = 1;
  auto fr1 = Cursor_Ptr{new FullCursor(ma1)};

  MeasArray ma2(2);
  ma2[0].time = 1;
  ma2[1].time = 6;
  ma2[0].flag = ma2[1].flag = 2;
  auto fr2 = Cursor_Ptr{new FullCursor(ma2)};

  MeasArray ma3(1);
  ma3[0].time = 7;
  ma3[0].flag = 3;
  auto fr3 = Cursor_Ptr{new FullCursor(ma3)};

  {
    auto tbl =
        Join::makeTable(CursorsList{fr1, fr2, fr3}, dariadb::IdArray{0, 1, 2});

    BOOST_CHECK_EQUAL(tbl.size(), size_t(7));
    dariadb::Time t = 0;
    for (const auto &row : tbl) {
      t++;
      for (const auto &v : row) {
        BOOST_CHECK_EQUAL(v.time, t);
      }
    }
  }
}