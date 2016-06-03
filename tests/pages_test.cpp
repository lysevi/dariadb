#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <algorithm>
#include <boost/test/unit_test.hpp>

#include <compression.h>
#include <storage/bloom_filter.h>
#include <storage/chunk.h>
#include <storage/manifest.h>
#include <storage/page.h>
#include <storage/page_manager.h>
#include <utils/fs.h>
#include <utils/utils.h>

using dariadb::storage::PageManager;
using dariadb::storage::Manifest;

BOOST_AUTO_TEST_CASE(ManifestFileTest) {
  const std::string fname = "manifest";
  if (dariadb::utils::fs::path_exists(fname)) {
    dariadb::utils::fs::rm(fname);
  }

  Manifest::start(fname);
  std::list<std::string> pages_names{"1", "2", "3"};
  for (auto n : pages_names) {
    Manifest::instance()->page_append(n);
  }

  std::list<std::string> cola_names{"11", "22", "33"};
  for (auto n : cola_names) {
    Manifest::instance()->cola_append(n);
  }

  auto page_lst = Manifest::instance()->page_list();
  BOOST_CHECK_EQUAL(page_lst.size(), pages_names.size());
  BOOST_CHECK_EQUAL_COLLECTIONS(page_lst.begin(), page_lst.end(), pages_names.begin(),
                                pages_names.end());

  auto cola_lst = Manifest::instance()->cola_list();
  BOOST_CHECK_EQUAL(cola_lst.size(), cola_names.size());
  BOOST_CHECK_EQUAL_COLLECTIONS(cola_lst.begin(), cola_lst.end(), cola_names.begin(),
                                cola_names.end());

  Manifest::stop();
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
  PageManager::start(PageManager::Params(storagePath, 1, 1));
  BOOST_CHECK(PageManager::instance() != nullptr);
  PageManager::stop();
}

dariadb::Time add_meases(dariadb::Id id, dariadb::Time t, size_t count,
                         dariadb::Meas::MeasList &addeded) {
  dariadb::Meas first;
  first.id = id;
  first.time = t;

  for (size_t i = 0; i < count; i++, t++) {
    first.flag = dariadb::Flag(i);
    first.time = t;
    first.value = dariadb::Value(i);
    auto res = PageManager::instance()->append(first);
    addeded.push_back(first);
    BOOST_CHECK(res.writed == 1);
  }

  return t;
}

BOOST_AUTO_TEST_CASE(PageManagerReadWrite) {
  const std::string storagePath = "testStorage/";
  const size_t chinks_count = 30;
  const size_t chunks_size = 256;

  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }

  PageManager::start(PageManager::Params(storagePath, chinks_count, chunks_size));
  BOOST_CHECK(PageManager::instance() != nullptr);

  auto start_time = dariadb::Time(0);
  auto t = dariadb::Time(0);
  dariadb::Meas::MeasList addeded;
  const dariadb::Id id_count(2);
  dariadb::IdSet all_id_set;
  for (size_t i = 0; i < 3; i++) {
    auto cur_id = dariadb::Id(i % id_count);
    all_id_set.insert(cur_id);
    t = add_meases(cur_id, t, chinks_count, addeded);
  }

  dariadb::Time minTime(t);
  dariadb::IdArray all_id_array{all_id_set.begin(), all_id_set.end()};
  { // Chunks load
    // must return all of appended chunks;
    dariadb::storage::ChunksList all_chunks;

    PageManager::instance()->flush();
    auto links_list = PageManager::instance()->chunksByIterval(
        dariadb::storage::QueryInterval(all_id_array, 0, 0, t));
    PageManager::instance()->readLinks(links_list)->readAll(&all_chunks);

    std::map<dariadb::Id, dariadb::Meas::MeasList> id2meas;
    for (auto add : addeded) {
      id2meas[add.id].push_back(add);
    }
    /*std::sort(all_chunks.begin(), all_chunks.end(),
            [](const dariadb::storage::Chunk_Ptr&l, const dariadb::storage::Chunk_Ptr&r)
            {
                    return l->info->last.time < r->info->last.time;
            }
    );*/
    for (auto id : all_id_array) {
      std::set<uint64_t> visited_chunks{};
      dariadb::Meas::MeasList mlist;
      for (auto ch : all_chunks) {
        if (visited_chunks.find(ch->header->id) != visited_chunks.end()) {
          continue;
        }
        visited_chunks.insert(ch->header->id);
        if (!dariadb::storage::bloom_check(ch->header->id_bloom, id)) {
          continue;
        }
        minTime = std::min(minTime, ch->header->minTime);
        ch->bw->reset_pos();

        dariadb::compression::CopmressedReader crr(ch->bw, ch->header->first);
        if (ch->header->first.id == id) {
          mlist.push_back(ch->header->first);
        }
        for (uint32_t i = 0; i < ch->header->count; i++) {
          auto m = crr.read();
          if (m.id != id) {
            continue;
          }
          mlist.push_back(m);
        }
      }
      auto addeded_lst = id2meas[id];
      auto addeded_iter = addeded_lst.cbegin();
      for (auto &m : mlist) {
        BOOST_CHECK_EQUAL(m.time, addeded_iter->time);
        BOOST_CHECK_EQUAL(m.value, addeded_iter->value);
        ++addeded_iter;
      }
    }
    /*dariadb::Time minT = dariadb::MAX_TIME,
                  maxT = dariadb::MIN_TIME;
    BOOST_CHECK(PageManager::instance()->minMaxTime(dariadb::Id(0), &minT, &maxT));
    BOOST_CHECK_EQUAL(minT, dariadb::Time(0));*/

    {
      dariadb::Time end_time(t / 2);
      dariadb::storage::ChunksList chunk_list;
      auto link_list = PageManager::instance()->chunksByIterval(
          dariadb::storage::QueryInterval(all_id_array, 0, start_time, end_time));
      PageManager::instance()->readLinks(link_list)->readAll(&chunk_list);

      for (auto &v : chunk_list) {
        BOOST_CHECK(v->header->minTime <= end_time);
      }

      auto id2meas_res = PageManager::instance()->valuesBeforeTimePoint(
          dariadb::storage::QueryTimePoint(all_id_array, 0, end_time));
      BOOST_CHECK_EQUAL(id2meas_res.size(), all_id_array.size());

      for (auto &kv : id2meas_res) {
        BOOST_CHECK(kv.second.time <= end_time);
      }

      /* auto ids_array = PageManager::instance()->getIds();
       BOOST_CHECK_EQUAL(ids_array.size(), size_t(2));
       BOOST_CHECK(std::find(ids_array.begin(), ids_array.end(), dariadb::Id(0)) !=
                   ids_array.end());
       BOOST_CHECK(std::find(ids_array.begin(), ids_array.end(), dariadb::Id(1)) !=
                   ids_array.end());*/
    }
  }
  BOOST_CHECK(dariadb::utils::fs::path_exists(storagePath));
  BOOST_CHECK(dariadb::utils::fs::ls(storagePath).size() == 3); // page +index+manifest

  PageManager::stop();

  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
}

BOOST_AUTO_TEST_CASE(PageManagerReadWriteWithContinue) {
  const std::string storagePath = "testStorage";
  const size_t chunks_count = 10;
  const size_t chunks_size = 200;
  auto t = dariadb::Time(0);

  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
  dariadb::Meas::MeasList addeded;
  PageManager::start(PageManager::Params(storagePath, chunks_count, chunks_size));
  dariadb::Meas first;
  first.id = 1;
  first.time = t;
  { add_meases(1, t, chunks_size / 10, addeded); }
  PageManager::stop();

  auto fname = dariadb::utils::fs::ls(storagePath, ".page").front();
  auto header = dariadb::storage::Page::readHeader(fname);
  BOOST_CHECK_EQUAL(header.chunk_per_storage, chunks_count);
  BOOST_CHECK_EQUAL(header.chunk_size, chunks_size);
  BOOST_CHECK_EQUAL(header.count_readers, size_t(0));

  auto iheader = dariadb::storage::Page::readIndexHeader(fname + "i");
  BOOST_CHECK_EQUAL(iheader.chunk_per_storage, chunks_count);
  BOOST_CHECK_EQUAL(iheader.chunk_size, chunks_size);
  BOOST_CHECK(iheader.is_sorted);

  PageManager::start(PageManager::Params(storagePath, chunks_count, chunks_size));

  auto mintime_chunks =
      PageManager::instance()->valuesBeforeTimePoint(dariadb::storage::QueryTimePoint(
          dariadb::IdArray{1}, 0, PageManager::instance()->minTime()));
  BOOST_CHECK_GE(mintime_chunks.size(), size_t(1));

  PageManager::stop();

  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
}

BOOST_AUTO_TEST_CASE(PageManagerMultiPageRead) {
  const std::string storagePath = "testStorage";
  const size_t chunks_count = 10;
  const size_t chunks_size = 200;
  auto t = dariadb::Time(0);

  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
  dariadb::Meas::MeasList addeded;
  PageManager::start(PageManager::Params(storagePath, chunks_count, chunks_size));
  dariadb::Meas first;
  first.id = 1;
  first.time = t;
  const size_t page_count = 4;

  while (dariadb::utils::fs::ls(storagePath, ".page").size() <= page_count) {
    t = add_meases(1, t, chunks_size / 10, addeded);
  }

  dariadb::storage::QueryInterval qi(dariadb::IdArray{1}, 0, addeded.front().time,
                                     addeded.back().time);

  dariadb::storage::QueryTimePoint qt(
      dariadb::IdArray{1}, 0,
      addeded.front().time + (addeded.back().time - addeded.front().time) / 2);

  dariadb::storage::ChunksList chlist;
  auto link_list = PageManager::instance()->chunksByIterval(qi);
  PageManager::instance()->readLinks(link_list)->readAll(&chlist);

  size_t writed = addeded.size();
  size_t readed = 0;
  std::map<uint64_t, size_t> chunk_id_to_count;
  dariadb::Meas::MeasList readed_lst;
  for (auto ch : chlist) {
    chunk_id_to_count[ch->header->id]++;
    ch->bw->reset_pos();

    dariadb::compression::CopmressedReader crr(ch->bw, ch->header->first);
    BOOST_CHECK_EQUAL(ch->header->first.time, addeded.front().time);
    addeded.pop_front();
    readed++;
    for (uint32_t i = 0; i < ch->header->count; i++) {
      auto m = crr.read();
      readed++;
      auto a = addeded.front();
      BOOST_CHECK_EQUAL(m.time, a.time);
      addeded.pop_front();
      readed_lst.push_back(m);
    }
  }
  for (auto kv : chunk_id_to_count) {
    BOOST_CHECK_EQUAL(kv.second, size_t(1));
  }
  BOOST_CHECK_EQUAL(readed, writed);

  auto id2meas = PageManager::instance()->valuesBeforeTimePoint(qt);

  BOOST_CHECK_EQUAL(id2meas.size(), qt.ids.size());
  for (auto kv : id2meas) {
    BOOST_CHECK_LE(kv.second.time, qt.time_point);
  }

  // TODO restore
  /*dariadb::Time minTime, maxTime;
  if (PageManager::instance()->minMaxTime(1, &minTime, &maxTime)) {
    BOOST_CHECK_EQUAL(minTime, qi.from);
    BOOST_CHECK_EQUAL(maxTime, qi.to);
  } else {
    BOOST_ERROR("PageManager::instance()->minMaxTime error!");
  }*/

  // auto ids = PageManager::instance()->getIds();
  // BOOST_CHECK_EQUAL(ids.size(), size_t(1));

  PageManager::stop();

  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
}

BOOST_AUTO_TEST_CASE(PageManagerPastReadWrite) {
  const std::string storagePath = "testStorage/";
  const size_t chinks_count = 30;
  const size_t chunks_size = 256;

  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }

  PageManager::start(PageManager::Params(storagePath, chinks_count, chunks_size));
  BOOST_CHECK(PageManager::instance() != nullptr);

  auto t = dariadb::Time(0);
  dariadb::Meas::MeasList addeded;

  dariadb::Meas first;
  first.id = 0;
  first.time = t;

  size_t i = 0;
  for (; i < 10; i++, t++) {
    if (t == 7 || t == 8) {
      continue;
    }
    first.flag = dariadb::Flag(i);
    first.time = t;
    first.value = dariadb::Value(i);

    auto res = PageManager::instance()->append(first);
    addeded.push_back(first);
    BOOST_CHECK(res.writed == 1);
  }
  first.id = 1;
  first.time = t;
  while (PageManager::instance()->chunks_in_cur_page() != 2) {
    first.flag = dariadb::Flag(i);
    first.time++;
    first.value = dariadb::Value(i);

    auto res = PageManager::instance()->append(first);
    addeded.push_back(first);
    BOOST_CHECK(res.writed == 1);
    ++i;
  }

  first.id = 0;
  first.time = 7;

  auto res = PageManager::instance()->append(first);
  addeded.push_back(first);
  BOOST_CHECK(res.writed == 1);

  first.time = 8;
  res = PageManager::instance()->append(first);
  addeded.push_back(first);
  BOOST_CHECK(res.writed == 1);

  first.time = 12;

  res = PageManager::instance()->append(first);
  addeded.push_back(first);
  BOOST_CHECK(res.writed == 1);

  auto links = PageManager::instance()->chunksByIterval(
      dariadb::storage::QueryInterval(dariadb::IdArray{0, 1}, 0, 0, 12));
  auto chunks_cursor = PageManager::instance()->readLinks(links);
  dariadb::storage::ChunksList chunks;
  chunks_cursor->readAll(&chunks);
  // chunks.back()->info->is_past_write = true;
  auto crossed = dariadb::storage::chunk_intercross(chunks);
  BOOST_CHECK_EQUAL(crossed.size(), size_t(2));

  auto reader = crossed.front()->get_reader();
  auto prev_value = reader->readNext();
  while (!reader->is_end()) {
    auto new_value = reader->readNext();
    BOOST_CHECK_LE(prev_value.time, new_value.time);
    prev_value = new_value;
  }

  PageManager::stop();
  if (dariadb::utils::fs::path_exists(storagePath)) {
    dariadb::utils::fs::rm(storagePath);
  }
}
