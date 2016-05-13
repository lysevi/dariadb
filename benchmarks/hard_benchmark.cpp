#include <cstdlib>
#include <ctime>
#include <iostream>
#include <iterator>

#include <dariadb.h>
#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <ctime>
#include <limits>
#include <random>
#include <thread>
#include <utils/fs.h>

class BenchCallback : public dariadb::storage::ReaderClb {
public:
  BenchCallback() = default;
  void call(const dariadb::Meas &m) {
    if (m.flag != dariadb::Flags::_NO_DATA) {
      count++;
    } else {
      count_ig++;
    }
  }
  size_t count;
  size_t count_ig;
};

void writer_1(dariadb::storage::MeasStorage_ptr ms) {
  auto m = dariadb::Meas::empty();
  dariadb::Time t = dariadb::timeutil::current_time();
  for (dariadb::Id i = 0; i < 32768; i += 1) {
    m.id = i;
    m.flag = dariadb::Flag(0);
    m.time = t;
    m.value = dariadb::Value(i);
    ms->append(m);
    t++;
  }
}

std::atomic_long writen{0};

void writer_2(dariadb::Id id_from, size_t id_per_thread,
              dariadb::storage::MeasStorage_ptr ms) {
  auto m = dariadb::Meas::empty();
  std::random_device r;
  std::default_random_engine e1(r());
  std::uniform_int_distribution<int> uniform_dist(10, 64);

  size_t max_id = (id_from + id_per_thread);

  for (dariadb::Id i = id_from; i < max_id; i += 1) {
    dariadb::Value v = 1.0;
    m.id = i;
    m.flag = dariadb::Flag(0);
    auto max_rnd = uniform_dist(e1);
    m.time = dariadb::timeutil::current_time();
    for (dariadb::Time p = 0; p < dariadb::Time(max_rnd); p++) {
      m.time += 10;
      m.value = v;
      ms->append(m);
      writen++;
      auto rnd = rand() / float(RAND_MAX);

      v += rnd;
    }
  }
}

int main(int argc, char *argv[]) {
  (void)argc;
  (void)argv;
  srand(static_cast<unsigned int>(time(NULL)));

  const std::string storage_path = "testStorage";
  const size_t chunk_per_storage = 1024 * 1024;
  const size_t chunk_size = 256;
  const dariadb::Time old_mem_chunks = 0;
  const size_t cap_B = 128 * 1024 / chunk_size;
  const size_t max_mem_chunks = 0;

  { // 1.
    if (dariadb::utils::fs::path_exists(storage_path)) {
      dariadb::utils::fs::rm(storage_path);
    }

    auto raw_ptr = new dariadb::storage::Engine(
        dariadb::storage::PageManager::Params(storage_path,
                                              chunk_per_storage, chunk_size),
        dariadb::storage::Capacitor::Params(cap_B, storage_path),
        dariadb::storage::Engine::Limits(old_mem_chunks, max_mem_chunks));
    dariadb::storage::MeasStorage_ptr ms{raw_ptr};
    auto start = clock();

    writer_1(ms);

    auto elapsed = ((float)clock() - start) / CLOCKS_PER_SEC;
    std::cout << "1. insert : " << elapsed << std::endl;
  }

  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }

  auto raw_ptr_ds = new dariadb::storage::Engine(
      dariadb::storage::PageManager::Params(storage_path,
                                            chunk_per_storage, chunk_size),
      dariadb::storage::Capacitor::Params(cap_B, storage_path),
      dariadb::storage::Engine::Limits(old_mem_chunks, max_mem_chunks));
  dariadb::storage::MeasStorage_ptr ms{raw_ptr_ds};

  { // 2.
    const size_t threads_count = 16;
    const size_t id_per_thread = size_t(32768 / threads_count);

    auto start = clock();
    std::vector<std::thread> writers(threads_count);
    size_t pos = 0;
    for (size_t i = 0; i < threads_count; i++) {
      std::thread t{writer_2, id_per_thread * i + 1, id_per_thread, ms};
      writers[pos++] = std::move(t);
    }

    pos = 0;
    for (size_t i = 0; i < threads_count; i++) {
      std::thread t = std::move(writers[pos++]);
      t.join();
    }

    auto elapsed = ((float)clock() - start) / CLOCKS_PER_SEC;
    std::cout << "2. insert : " << elapsed << std::endl;
    raw_ptr_ds->flush();
  }
  auto queue_sizes = raw_ptr_ds->queue_size();
  std::cout << "\r"
            << " in disk chunks: "
            << dariadb::storage::PageManager::instance()->chunks_in_cur_page()
            << " in queue: (p:" << queue_sizes.page
            << " cap:" << queue_sizes.cap << ")"
            << std::endl;
  /* {
       auto ids=ms->getIds();
       std::cout << "ids.size:"<<ids.size() << std::endl;
       std::cout << "read all..." << std::endl;
       std::shared_ptr<BenchCallback> clbk{ new BenchCallback() };
       auto start = clock();
       ms->readInterval(0,ms->maxTime())->readAll(clbk.get());

       auto elapsed = (((float)clock() - start) / CLOCKS_PER_SEC);
       std::cout << "readed: " << clbk->count << std::endl;
       std::cout << "time: " << elapsed << std::endl;
   }*/
  /*{ // 3

    std::random_device r;
    std::default_random_engine e1(r());
    std::uniform_int_distribution<dariadb::Id> uniform_dist(1, 32767);
    dariadb::IdArray ids;
    ids.resize(1);

    const size_t queries_count = 32768;

    dariadb::IdArray rnd_ids(queries_count);
    std::vector<dariadb::Time> rnd_time(queries_count);
    for (size_t i = 0; i < queries_count; i++) {
      rnd_ids[i] = uniform_dist(e1);
      dariadb::Time minT, maxT;
      bool exists = raw_ptr_ds->minMaxTime(rnd_ids[i], &minT, &maxT);
      if (!exists) {
        throw MAKE_EXCEPTION("!exists");
      }
      std::uniform_int_distribution<dariadb::Time> uniform_dist_tmp(minT, maxT);
      rnd_time[i] = uniform_dist_tmp(e1);
    }
    auto raw_ptr = new BenchCallback();
    dariadb::storage::ReaderClb_ptr clbk{raw_ptr};

    auto start = clock();

    for (size_t i = 0; i < queries_count; i++) {
      ids[0] = rnd_ids[i];
      auto t = rnd_time[i];
      auto rdr =
          ms->readInTimePoint(dariadb::storage::QueryTimePoint(ids, 0, t));
      rdr->readAll(clbk.get());
    }

    auto elapsed = (((float)clock() - start) / CLOCKS_PER_SEC) / queries_count;
    std::cout << "3. time point: " << elapsed << " readed: " << raw_ptr->count
              << " ignored: " << raw_ptr->count_ig << std::endl;
  }*/
  { // 4
    std::random_device r;
    std::default_random_engine e1(r());
    std::uniform_int_distribution<dariadb::Id> uniform_dist(
        raw_ptr_ds->minTime(), dariadb::timeutil::current_time());

    const size_t queries_count = 32;

    dariadb::IdArray ids;
    std::vector<dariadb::Time> rnd_time(queries_count);
    for (size_t i = 0; i < queries_count; i++) {
      rnd_time[i] = uniform_dist(e1);
    }
    auto raw_ptr = new BenchCallback();
    dariadb::storage::ReaderClb_ptr clbk{raw_ptr};

    auto start = clock();

    for (size_t i = 0; i < queries_count; i++) {
      auto t = rnd_time[i];
      auto rdr =
          ms->readInTimePoint(dariadb::storage::QueryTimePoint(ids, 0, t));
      rdr->readAll(clbk.get());
    }

    auto elapsed = (((float)clock() - start) / CLOCKS_PER_SEC) / queries_count;
    std::cout << "4. time point: " << elapsed << " readed: " << raw_ptr->count
              << std::endl;
  }

  { // 5
    std::random_device r;
    std::default_random_engine e1(r());
    std::uniform_int_distribution<dariadb::Time> uniform_dist(
        raw_ptr_ds->minTime(), dariadb::timeutil::current_time());

    auto raw_ptr = new BenchCallback();
    dariadb::storage::ReaderClb_ptr clbk{raw_ptr};
    const size_t queries_count = 1;

    std::vector<dariadb::Time> rnd_time_from(queries_count),
        rnd_time_to(queries_count);
    for (size_t i = 0; i < queries_count; i++) {
      rnd_time_from[i] = uniform_dist(e1);
      rnd_time_to[i] = uniform_dist(e1);
    }

    auto start = clock();

    for (size_t i = 0; i < queries_count; i++) {
      auto f = std::min(rnd_time_from[i], rnd_time_to[i]);
      auto t = std::max(rnd_time_from[i], rnd_time_to[i]);
      auto rdr = ms->readInterval(f, t);
      rdr->readAll(clbk.get());
    }

    auto elapsed = (((float)clock() - start) / CLOCKS_PER_SEC) / queries_count;
    std::cout << "5. interval: " << elapsed << " readed: " << raw_ptr->count
              << std::endl;
  }

  { // 6
    std::random_device r;
    std::default_random_engine e1(r());
    std::uniform_int_distribution<dariadb::Time> uniform_dist(
        raw_ptr_ds->minTime(), dariadb::timeutil::current_time());
    std::uniform_int_distribution<dariadb::Id> uniform_dist_id(1, 32767);

    const size_t ids_count = size_t(32768 * 0.1);
    dariadb::IdArray ids;
    ids.resize(ids_count);

    auto raw_ptr = new BenchCallback();
    dariadb::storage::ReaderClb_ptr clbk{raw_ptr};
    const size_t queries_count = 32;

    std::vector<dariadb::Time> rnd_time_from(queries_count),
        rnd_time_to(queries_count);
    for (size_t i = 0; i < queries_count; i++) {
      rnd_time_from[i] = uniform_dist(e1);
      rnd_time_to[i] = uniform_dist(e1);
    }

    auto start = clock();

    for (size_t i = 0; i < queries_count; i++) {
      for (size_t j = 0; j < ids_count; j++) {
        ids[j] = uniform_dist_id(e1);
      }
      auto f = std::min(rnd_time_from[i], rnd_time_to[i]);
      auto t = std::max(rnd_time_from[i], rnd_time_to[i]);
      auto rdr =
          ms->readInterval(dariadb::storage::QueryInterval(ids, 0, f, t));
      rdr->readAll(clbk.get());
    }

    auto elapsed = (((float)clock() - start) / CLOCKS_PER_SEC) / queries_count;
    std::cout << "6. interval: " << elapsed << " readed: " << raw_ptr->count
              << std::endl;
  }

  ms = nullptr;

  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}
