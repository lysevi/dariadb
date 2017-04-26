#include <gtest/gtest.h>

#include "helpers.h"

#include <libdariadb/interfaces/icursor.h>
#include <libdariadb/storage/bloom_filter.h>
#include <libdariadb/storage/chunk.h>
#include <libdariadb/storage/cursors.h>
#include <libdariadb/storage/manifest.h>
#include <libdariadb/utils/fs.h>

#include <iostream>

TEST(Common, MeasTest) {
  dariadb::Meas m;
  m.flag = dariadb::Flag(1);
  EXPECT_TRUE(!m.inFlag(dariadb::Flag(2)));
  m.flag = 3;
  EXPECT_TRUE(m.inFlag(dariadb::Flag(1)));
}

TEST(Common, BloomTest) {
  uint64_t u8_fltr = dariadb::storage::bloom_empty<uint8_t>();

  EXPECT_EQ(u8_fltr, uint64_t{0});

  u8_fltr = dariadb::storage::bloom_add(u8_fltr, uint8_t{1});
  u8_fltr = dariadb::storage::bloom_add(u8_fltr, uint8_t{2});
  u8_fltr = dariadb::storage::bloom_add(u8_fltr, uint8_t{3});

  EXPECT_TRUE(dariadb::storage::bloom_check(u8_fltr, uint8_t{1}));
  EXPECT_TRUE(dariadb::storage::bloom_check(u8_fltr, uint8_t{2}));
  EXPECT_TRUE(dariadb::storage::bloom_check(u8_fltr, uint8_t{3}));

  uint64_t u8_fltr_2 = dariadb::storage::bloom_empty<uint8_t>();
  u8_fltr_2 = dariadb::storage::bloom_add(u8_fltr_2, uint8_t{4});

  EXPECT_TRUE(!dariadb::storage::bloom_check(u8_fltr, uint8_t{4}));
  EXPECT_TRUE(dariadb::storage::bloom_check(u8_fltr_2, uint8_t{4}));

  auto super_fltr = dariadb::storage::bloom_combine(u8_fltr, u8_fltr_2);
  EXPECT_TRUE(dariadb::storage::bloom_check(super_fltr, uint8_t{1}));
  EXPECT_TRUE(dariadb::storage::bloom_check(super_fltr, uint8_t{2}));
  EXPECT_TRUE(dariadb::storage::bloom_check(super_fltr, uint8_t{3}));
  EXPECT_TRUE(dariadb::storage::bloom_check(super_fltr, uint8_t{4}));
}

TEST(Common, inFilter) {
  {
    auto m = dariadb::Meas();
    m.flag = 100;
    EXPECT_TRUE(m.inFlag(0));
    EXPECT_TRUE(m.inFlag(100));
    EXPECT_TRUE(!m.inFlag(10));
  }
}

TEST(Common, StatisticUpdate) {
  dariadb::Statistic st;

  EXPECT_EQ(st.minTime, dariadb::MAX_TIME);
  EXPECT_EQ(st.maxTime, dariadb::MIN_TIME);
  EXPECT_EQ(st.count, uint32_t(0));
  EXPECT_EQ(st.flag_bloom, dariadb::Flag(0));
  EXPECT_EQ(st.minValue, dariadb::MAX_VALUE);
  EXPECT_EQ(st.maxValue, dariadb::MIN_VALUE);
  EXPECT_EQ(st.sum, dariadb::Value(0));

  auto m = dariadb::Meas(0);
  m.time = 2;
  m.flag = 2;
  m.value = 2;
  st.update(m);
  EXPECT_EQ(st.minTime, m.time);
  EXPECT_EQ(st.maxTime, m.time);
  EXPECT_TRUE(st.flag_bloom != dariadb::Flag(0));
  EXPECT_TRUE(dariadb::areSame(st.minValue, m.value));
  EXPECT_TRUE(dariadb::areSame(st.maxValue, m.value));

  m.time = 3;
  m.value = 3;
  st.update(m);
  EXPECT_EQ(st.minTime, dariadb::Time(2));
  EXPECT_EQ(st.maxTime, dariadb::Time(3));
  EXPECT_TRUE(dariadb::areSame(st.minValue, dariadb::Value(2)));
  EXPECT_TRUE(dariadb::areSame(st.maxValue, dariadb::Value(3)));

  m.time = 1;
  m.value = 1;
  st.update(m);
  EXPECT_EQ(st.minTime, dariadb::Time(1));
  EXPECT_EQ(st.maxTime, dariadb::Time(3));
  EXPECT_TRUE(dariadb::areSame(st.minValue, dariadb::Value(1)));
  EXPECT_TRUE(dariadb::areSame(st.maxValue, dariadb::Value(3)));
  EXPECT_TRUE(dariadb::areSame(st.sum, dariadb::Value(6)));
  EXPECT_EQ(st.count, uint32_t(3));

  dariadb::Statistic second_st;
  m.time = 777;
  m.value = 1;
  second_st.update(m);
  EXPECT_EQ(second_st.maxTime, m.time);

  second_st.update(st);
  EXPECT_EQ(second_st.minTime, dariadb::Time(1));
  EXPECT_EQ(second_st.maxTime, dariadb::Time(777));
  EXPECT_TRUE(dariadb::areSame(second_st.minValue, dariadb::Value(1)));
  EXPECT_TRUE(dariadb::areSame(second_st.maxValue, dariadb::Value(3)));
  EXPECT_TRUE(dariadb::areSame(second_st.sum, dariadb::Value(7)));
  EXPECT_EQ(second_st.count, uint32_t(4));
}

TEST(Common, ManifestFileTest) {
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
    dariadb::Id id = 0;
    for (auto n : wal_names) {
      manifest->wal_append(n, id++);
    }

    auto page_lst = manifest->page_list();
    EXPECT_EQ(page_lst.size(), pages_names.size());

    auto page_lst_it = page_lst.begin();
    auto page_names_it = pages_names.begin();
    while (page_lst_it != page_lst.end() || page_names_it != pages_names.end()) {
      EXPECT_EQ(*page_lst_it, *page_names_it);
      ++page_lst_it;
      ++page_names_it;
    }

    auto wal_lst = manifest->wal_list();

    auto wal_lst_it = wal_lst.begin();
    auto wal_names_it = wal_names.begin();
    while (wal_lst_it != wal_lst.end() || wal_names_it != wal_names.end()) {
      EXPECT_EQ(wal_lst_it->fname, *wal_names_it);
      ++wal_lst_it;
      ++wal_names_it;
    }

    auto loaded_wal_names = manifest->wal_list(dariadb::Id(0));
    EXPECT_EQ(loaded_wal_names.size(), size_t(1));
	loaded_wal_names = manifest->wal_list(dariadb::Id(1));
    EXPECT_EQ(loaded_wal_names.size(), size_t(1));
	loaded_wal_names = manifest->wal_list(dariadb::Id(2));
    EXPECT_EQ(loaded_wal_names.size(), size_t(1));

    manifest = nullptr;
  }
  { // reopen. restore method must remove all records from manifest.
    auto settings = dariadb::storage::Settings::create(storage_path);
    auto manifest = dariadb::storage::Manifest::create(settings);
    EXPECT_EQ(manifest->page_list().size(), size_t(0));
    EXPECT_EQ(manifest->wal_list().size(), size_t(0));
    EXPECT_EQ(manifest->get_format(), version);
  }

  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

TEST(Common, SettingsInstance) {
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

  bool file_exists = dariadb::utils::fs::path_exists(dariadb::utils::fs::append_path(
      storage_path, dariadb::storage::SETTINGS_FILE_NAME));
  EXPECT_TRUE(file_exists);

  settings = dariadb::storage::Settings::create(storage_path);
  EXPECT_EQ(settings->wal_cache_size.value(), uint64_t(2));
  EXPECT_EQ(settings->chunk_size.value(), uint32_t(7));
  EXPECT_EQ(settings->max_pages_in_level.value(), uint16_t(10));
  EXPECT_TRUE(settings->strategy.value() == dariadb::STRATEGY::COMPRESSED);

  settings = nullptr;
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

TEST(Common, ChunkTest) {
  {
    dariadb::storage::ChunkHeader hdr;
    uint8_t *buff = new uint8_t[1024];
    std::fill_n(buff, 1024, uint8_t(0));
    auto m = dariadb::Meas();
    auto ch = dariadb::storage::Chunk::create(&hdr, buff, 1024, m);
    m.time = 0;
    size_t writed = 0;
    while (!ch->isFull()) {
      ch->append(m);
      ch->append(m);
      writed += 2;
      m.time++;
    }
    EXPECT_EQ(hdr.is_sorted, uint8_t(1));
    auto rdr = ch->getReader();
    EXPECT_EQ(rdr->count(), writed);
    dariadb_test::check_reader(rdr);
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
    EXPECT_EQ(hdr.is_sorted, uint8_t(0));
    auto rdr = ch->getReader();
    EXPECT_EQ(rdr->count(), writed);
    dariadb_test::check_reader(rdr);
    {
      auto skip_size = dariadb::storage::Chunk::compact(&hdr);
      EXPECT_TRUE(dariadb::storage::Chunk::compact(&hdr) == uint32_t(0));
      auto ch2 = dariadb::storage::Chunk::open(&hdr, buff + skip_size);
      auto rdr2 = ch2->getReader();
      size_t readed = 0;
      while (!rdr2->is_end()) {
        rdr2->readNext();
        readed++;
      }
      EXPECT_EQ(readed, writed);
    }

    delete[] buff;
  }
}

TEST(Common, FullCursorTest) {
  {
    dariadb::MeasArray ma;
    auto m = dariadb::Meas();
    m.time = 0;
    size_t writed = size_t(0);
    while (ma.size() < size_t(100)) {
      ma.push_back(m);
      ma.push_back(m);
      writed += 2;
      m.time++;
    }
    dariadb::Cursor_Ptr cptr(new dariadb::storage::FullCursor(ma));
    EXPECT_EQ(cptr->count(), writed);
    dariadb_test::check_reader(cptr);
  }
}

TEST(Common, LinearReaderTest) {
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
  EXPECT_EQ(lr.count(), fr1->count() + fr2->count());

  std::list<dariadb::Meas> ml;
  while (!lr.is_end()) {
    auto v = lr.readNext();
    ml.push_back(v);

    if (!lr.is_end()) {
      auto tp = lr.top();
      EXPECT_LT(v.time, tp.time);
    }
  }

  EXPECT_EQ(ml.size(), size_t(8));
}

TEST(Common, MergeSortReaderTest) {
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
  EXPECT_EQ(msr.count(), fr1->count() + fr2->count() + fr3->count());

  std::list<dariadb::Meas> ml;
  while (!msr.is_end()) {
    auto v = msr.readNext();
    ml.push_back(v);

    if (!msr.is_end()) {
      auto tp = msr.top();
      EXPECT_LT(v.time, tp.time);
    }
  }

  auto must_be_false =
      dariadb::storage::CursorWrapperFactory::is_linear_readers(fr1, fr2);
  auto must_be_true = dariadb::storage::CursorWrapperFactory::is_linear_readers(fr1, fr3);

  EXPECT_TRUE(must_be_true);
  EXPECT_TRUE(!must_be_false);

  EXPECT_EQ(ml.size(), size_t(8));

  for (auto it = ml.begin(); it != ml.end(); ++it) {
    auto cur_value = *it;
    auto next = std::next(it);
    if (next == ml.end()) {
      break;
    }
    auto next_value = *next;

    EXPECT_LT(cur_value.time, next_value.time);
  }
}

TEST(Common, ReaderColapseTest) {
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
    EXPECT_EQ(msr->count(), fr1->count() + fr2->count());
    auto top_reader = dynamic_cast<LinearCursor *>(msr.get());
    auto is_merge_reader =
        dynamic_cast<MergeSortCursor *>(top_reader->_readers.front().get()) != nullptr;
    EXPECT_TRUE(is_merge_reader);
    EXPECT_EQ(top_reader->_readers.size(), size_t(1));
  }

  {
    auto lsr = CursorWrapperFactory::colapseCursors(CursorsList{fr1, fr3});
    EXPECT_EQ(lsr->count(), fr1->count() + fr3->count());
    auto top_reader = dynamic_cast<LinearCursor *>(lsr.get());
    for (auto &r : top_reader->_readers) {
      auto is_full_reader = dynamic_cast<FullCursor *>(r.get()) != nullptr;
      EXPECT_TRUE(is_full_reader);
    }
  }
}

TEST(Common, ReaderColapse3Test) {
  using namespace dariadb::storage;
  using namespace dariadb;
  MeasArray ma1(4);
  ma1[0].time = 1;
  ma1[1].time = 2;
  ma1[2].time = 4;
  ma1[3].time = 5;
  auto fr1 = Cursor_Ptr{new FullCursor(ma1)};

  MeasArray ma2(4);
  ma2[0].time = 3;
  ma2[1].time = 5;
  ma2[2].time = 6;
  ma2[3].time = 7;
  auto fr2 = Cursor_Ptr{new FullCursor(ma2)};

  MeasArray ma3(2);
  ma3[0].time = 6;
  ma3[1].time = 9;
  auto fr3 = Cursor_Ptr{new FullCursor(ma3)};

  auto msr = CursorWrapperFactory::colapseCursors(CursorsList{fr1, fr2, fr3});
  EXPECT_EQ(msr->count(), fr1->count() + fr2->count() + fr3->count());
  while (msr->is_end()) {
    msr->readNext();
  }
}