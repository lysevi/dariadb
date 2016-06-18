#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main
#include <atomic>
#include <boost/test/unit_test.hpp>
#include <cassert>
#include <map>
#include <thread>

#include "test_common.h"
#include <storage/aofile.h>
#include <timeutil.h>
#include <utils/fs.h>

class Moc_Storage : public dariadb::storage::MeasWriter {
public:
  size_t writed_count;
  std::map<dariadb::Id, std::vector<dariadb::Meas>> meases;
  std::list<dariadb::Meas> mlist;
  dariadb::append_result append(const dariadb::Meas &value) override {
    meases[value.id].push_back(value);
    mlist.push_back(value);
    writed_count += 1;
    return dariadb::append_result(1, 0);
  }

  void flush() override {}
};

BOOST_AUTO_TEST_CASE(AofInitTest) {
  const size_t block_size = 1000;
  auto storage_path = "testStorage";
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }

  dariadb::utils::fs::mkdir(storage_path);
  auto aof_files = dariadb::utils::fs::ls(storage_path, dariadb::storage::AOF_FILE_EXT);
  assert(aof_files.size() == 0);
  auto p = dariadb::storage::AOFile::Params(block_size, dariadb::utils::fs::append_path(storage_path, "test.aof"));
  size_t writes_count = block_size;

  dariadb::IdSet id_set;
  {
    dariadb::storage::AOFile aof{p};

    aof_files = dariadb::utils::fs::ls(storage_path, dariadb::storage::AOF_FILE_EXT);
    BOOST_CHECK_EQUAL(aof_files.size(), size_t(0));

    auto e = dariadb::Meas::empty();

    size_t id_count = 10;

    size_t i = 0;
    e.id = i % id_count;
    id_set.insert(e.id);
    e.time = dariadb::Time(i);
    e.value = dariadb::Value(i);
    BOOST_CHECK(aof.append(e).writed == 1);
    i++;
    dariadb::Meas::MeasList ml;
    for (; i < writes_count/2; i++) {
      e.id = i % id_count;
      id_set.insert(e.id);
      e.time = dariadb::Time(i);
      e.value = dariadb::Value(i);
      ml.push_back(e);
    }
    aof.append(ml);

    dariadb::Meas::MeasArray ma;
    ma.resize(writes_count-i);
    size_t pos=0;
    for (; i < writes_count; i++) {
      e.id = i % id_count;
      id_set.insert(e.id);
      e.time = dariadb::Time(i);
      e.value = dariadb::Value(i);
      ma[pos]=e;
      pos++;
    }
    aof.append(ma);
    aof_files = dariadb::utils::fs::ls(storage_path, dariadb::storage::AOF_FILE_EXT);
    BOOST_CHECK_EQUAL(aof_files.size(), size_t(1));

    dariadb::Meas::MeasList out;

    auto reader = aof.readInterval(dariadb::storage::QueryInterval(
        dariadb::IdArray(id_set.begin(), id_set.end()), 0, 0, writes_count));
    BOOST_CHECK(reader != nullptr);
    reader->readAll(&out);
    BOOST_CHECK_EQUAL(out.size(), writes_count);
  }

  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

BOOST_AUTO_TEST_CASE(CapacitorCommonTest) {
  const size_t block_size = 10;
  auto storage_path = "testStorage";
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
  {
    dariadb::utils::fs::mkdir(storage_path);
    auto aof_files = dariadb::utils::fs::ls(storage_path, dariadb::storage::AOF_FILE_EXT);
    assert(aof_files.size() == 0);
    auto p = dariadb::storage::AOFile::Params(block_size, dariadb::utils::fs::append_path(storage_path, "test.aof"));
    dariadb::storage::AOFile aof(p);

    dariadb_test::storage_test_check(&aof, 0, 100, 1);
  }
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}
/*
BOOST_AUTO_TEST_CASE(CapacitorIsFull) {
	const size_t block_size = 10;
	auto storage_path = "testStorage";
	if (dariadb::utils::fs::path_exists(storage_path)) {
		dariadb::utils::fs::rm(storage_path);
	}
	std::shared_ptr<Moc_Storage> stor(new Moc_Storage);
	stor->writed_count = 0;

	auto cap_files = dariadb::utils::fs::ls(storage_path, dariadb::storage::CAP_FILE_EXT);
	assert(cap_files.size() == 0);
	auto p = dariadb::storage::Capacitor::Params(block_size, storage_path);
	p.max_levels = 11;
	{
		dariadb::storage::Manifest::start(
			dariadb::utils::fs::append_path(storage_path, "Manifest"));
		dariadb::storage::Capacitor cap(p);

		auto e = dariadb::Meas::empty();

		size_t id_count = 10;
		size_t addeded = 0;
		for (size_t i = 0;; i++) {
			e.id = i % id_count;
			e.time++;
			e.value = dariadb::Value(i);
			if (cap.append(e).ignored != 0) {
				break;
			}
			addeded++;
		}
		BOOST_CHECK_GT(addeded, size_t(0));
	}
	auto clist = dariadb::storage::Manifest::instance()->cola_list();
	BOOST_CHECK_EQUAL(clist.size(), size_t(1));
	auto hdr = dariadb::storage::Capacitor::readHeader(dariadb::utils::fs::append_path(storage_path, clist.front()));
	BOOST_CHECK(hdr.is_closed = true);
	BOOST_CHECK(hdr.is_full = true);
	dariadb::storage::Manifest::stop();
}

BOOST_AUTO_TEST_CASE(CapReadIntervalTest) {
  std::shared_ptr<Moc_Storage> stor(new Moc_Storage);
  stor->writed_count = 0;
  const size_t block_size = 10;
  auto storage_path = "testStorage";
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }

  {
    auto p = dariadb::storage::Capacitor::Params(block_size, storage_path);
    p.max_levels = 11;
    dariadb::storage::Manifest::start(
        dariadb::utils::fs::append_path(storage_path, "Manifest"));
    dariadb::storage::Capacitor cap(p);

    dariadb_test::readIntervalCommonTest(&cap);
    dariadb::storage::Manifest::stop();
  }

  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

std::atomic_size_t append_count{0};

void thread_writer(dariadb::Id id, dariadb::Time from, dariadb::Time to,
                   dariadb::Time step, dariadb::storage::Capacitor *cp) {
  const size_t copies_count = 1;
  auto m = dariadb::Meas::empty();
  m.time = from;
  for (auto i = from; i < to; i += step) {
    m.id = id;
    m.flag = dariadb::Flag(i);
    m.value = 0;
    for (size_t j = 0; j < copies_count; j++) {
      ++m.time;
      m.value = dariadb::Value(j);
      cp->append(m);
      append_count++;
    }
  }
}

BOOST_AUTO_TEST_CASE(MultiThread) {
  const std::string storage_path = "testStorage";
  const size_t max_size = 10;
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
  {
    dariadb::storage::Manifest::start(
        dariadb::utils::fs::append_path(storage_path, "Manifest"));
    dariadb::storage::Capacitor mbucket{dariadb::storage::Capacitor::Params(max_size, storage_path)};

    std::thread t1(thread_writer, 0, 0, 10, 1, &mbucket);
    std::thread t2(thread_writer, 1, 0, 10, 1, &mbucket);
    std::thread t3(thread_writer, 2, 0, 100, 2, &mbucket);
    std::thread t4(thread_writer, 3, 0, 100, 1, &mbucket);

    t1.join();
    t2.join();
    t3.join();
    t4.join();

    mbucket.flush();
    dariadb::Meas::MeasList out;
    dariadb::IdArray all_id{0, 1, 2, 3};
    dariadb::storage::QueryInterval qi(all_id, 0, 0, 100);
    mbucket.readInterval(qi)->readAll(&out);
    BOOST_CHECK_EQUAL(out.size(), append_count);
    dariadb::storage::Manifest::stop();
  }
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

BOOST_AUTO_TEST_CASE(byStep) {
  const size_t max_size = 10;
  auto storage_path = "testStorage";

  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }

  const size_t id_count = 1;
  { // equal step
    dariadb::storage::Manifest::start(
        dariadb::utils::fs::append_path(storage_path, "Manifest"));
    dariadb::storage::Capacitor ms{dariadb::storage::Capacitor::Params(max_size, storage_path)};

    auto m = dariadb::Meas::empty();
    const size_t total_count = 100;
    const dariadb::Time time_step = 1;
    dariadb::IdSet all_id;
    for (size_t i = 0; i < total_count; i += time_step) {
      m.id = i % id_count;
      all_id.insert(m.id);
      m.flag = dariadb::Flag(i);
      m.time = i;
      m.value = 0;
      ms.append(m);
    }
    dariadb::storage::QueryInterval q_all(dariadb::IdArray{all_id.begin(), all_id.end()},
                                          0, 0, total_count);
    auto rdr = ms.readInterval(q_all);

    dariadb::Meas::MeasList allByStep;
    rdr = ms.readInterval(q_all);
    rdr->readByStep(&allByStep, 0, total_count, time_step);
    auto expected = size_t(total_count / time_step) * id_count; //+ timepoint
    BOOST_CHECK_EQUAL(allByStep.size(), expected);
    dariadb::storage::Manifest::stop();
  }

  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }

  { // less step
    dariadb::storage::Manifest::start(
        dariadb::utils::fs::append_path(storage_path, "Manifest"));
    dariadb::storage::Capacitor ms{dariadb::storage::Capacitor::Params(max_size, storage_path)};

    auto m = dariadb::Meas::empty();
    const size_t total_count = 100;
    const dariadb::Time time_step = 10;
    dariadb::IdSet all_id;
    for (size_t i = 0; i < total_count; i += time_step) {
      m.id = i % id_count;
      all_id.insert(m.id);
      m.flag = dariadb::Flag(i);
      m.time = i;
      m.value = 0;
      ms.append(m);
    }

    dariadb::storage::QueryInterval q_all(dariadb::IdArray{all_id.begin(), all_id.end()},
                                          0, 0, total_count);
    auto rdr = ms.readInterval(q_all);

    dariadb::Time query_step = 11;
    dariadb::Meas::MeasList allByStep;
    rdr = ms.readInterval(q_all);
    rdr->readByStep(&allByStep, 0, total_count, query_step);
    auto expected = size_t(total_count / query_step) * id_count + id_count; //+ timepoint;
    BOOST_CHECK_EQUAL(allByStep.size(), expected);
    dariadb::storage::Manifest::stop();
  }

  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }

  { // great step
    dariadb::storage::Manifest::start(
        dariadb::utils::fs::append_path(storage_path, "Manifest"));
    dariadb::storage::Capacitor ms{dariadb::storage::Capacitor::Params(max_size, storage_path)};

    auto m = dariadb::Meas::empty();
    const size_t total_count = 100;
    const dariadb::Time time_step = 10;
    dariadb::IdSet all_id;

    for (size_t i = 0; i < total_count; i += time_step) {
      m.id = i % id_count;
      all_id.insert(m.id);
      m.flag = dariadb::Flag(i);
      m.time = i;
      m.value = 0;
      ms.append(m);
    }
    dariadb::storage::QueryInterval q_all(dariadb::IdArray{all_id.begin(), all_id.end()},
                                          0, 0, total_count);
    auto rdr = ms.readInterval(q_all);
    dariadb::Meas::MeasList all;
    rdr->readAll(&all);

    dariadb::Time query_step = 5;
    dariadb::Meas::MeasList allByStep;
    rdr = ms.readInterval(q_all);
    rdr->readByStep(&allByStep, 0, total_count, query_step);
    auto expected =
        size_t(total_count / time_step) * 2 * id_count + id_count; //+ timepoint;
    BOOST_CHECK_EQUAL(allByStep.size(), expected);
    dariadb::storage::Manifest::stop();
  }
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
  { // from before data
    dariadb::storage::Manifest::start(
        dariadb::utils::fs::append_path(storage_path, "Manifest"));
    dariadb::storage::Capacitor ms{dariadb::storage::Capacitor::Params(max_size, storage_path)};

    auto m = dariadb::Meas::empty();
    const size_t total_count = 100;
    const dariadb::Time time_step = 10;
    dariadb::IdSet all_id;
    for (size_t i = time_step; i < total_count; i += time_step) {
      m.id = i % id_count;
      all_id.insert(m.id);
      m.flag = dariadb::Flag(i);
      m.time = i;
      m.value = 0;
      ms.append(m);
    }
    dariadb::storage::QueryInterval q_all(dariadb::IdArray{all_id.begin(), all_id.end()},
                                          0, time_step, total_count);
    auto rdr = ms.readInterval(q_all);
    dariadb::Meas::MeasList all;
    rdr->readAll(&all);

    dariadb::Time query_step = 5;
    dariadb::Meas::MeasList allByStep;
    rdr = ms.readInterval(q_all);

    rdr->readByStep(&allByStep, 0, total_count, query_step);

    dariadb::Time expected = dariadb::Time((total_count - time_step) / time_step) * 2;
    expected = expected * id_count;
    expected += id_count * (time_step / query_step); //+ before first value
    expected += id_count;                            // one after last  value

    BOOST_CHECK_EQUAL(allByStep.size(), expected);
    dariadb::storage::Manifest::stop();
  }
  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

class Moc_I1 : public dariadb::statistic::BaseMethod {
public:
  Moc_I1() { _a = _b = dariadb::Meas::empty(); }
  void calc(const dariadb::Meas &a, const dariadb::Meas &b) override {
    _a = a;
    _b = b;
  }
  dariadb::Value result() const override { return dariadb::Value(); }
  dariadb::Meas _a;
  dariadb::Meas _b;
};

BOOST_AUTO_TEST_CASE(CallCalc) {
  std::unique_ptr<Moc_I1> p{new Moc_I1};
  auto m = dariadb::Meas::empty();
  m.time = 1;
  p->call(m);
  BOOST_CHECK_EQUAL(p->_a.time, dariadb::Time(0));
  m.time = 2;

  p->call(m);
  BOOST_CHECK_EQUAL(p->_a.time, dariadb::Time(1));
  BOOST_CHECK_EQUAL(p->_b.time, dariadb::Time(2));

  m.time = 3;
  p->call(m);
  BOOST_CHECK_EQUAL(p->_a.time, dariadb::Time(2));
  BOOST_CHECK_EQUAL(p->_b.time, dariadb::Time(3));
  std::shared_ptr<Moc_Storage> stor(new Moc_Storage);
  stor->writed_count = 0;
  const size_t max_size = 10;
  auto storage_path = "testStorage";

  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }

  { // equal step
    dariadb::storage::Manifest::start(
        dariadb::utils::fs::append_path(storage_path, "Manifest"));
    dariadb::storage::Capacitor ms{dariadb::storage::Capacitor::Params(max_size, storage_path)};

    using dariadb::statistic::average::Average;

    m = dariadb::Meas::empty();
    const size_t total_count = 100;
    const dariadb::Time time_step = 1;
    dariadb::IdSet all_id;
    for (size_t i = 0; i < total_count; i += time_step) {
      m.id = 1;
      all_id.insert(m.id);
      m.flag = dariadb::Flag(i);
      m.time = i;
      m.value = 5;
      ms.append(m);
    }

    ms.flush();
    std::unique_ptr<Average> p_average{new Average()};

    dariadb::storage::QueryInterval q_all(dariadb::IdArray{all_id.begin(), all_id.end()},
                                          0, 0, total_count);

    auto rdr = ms.readInterval(q_all);

    p_average->fromReader(rdr, 0, total_count, 1);
    BOOST_CHECK_CLOSE(p_average->result(), dariadb::Value(5), 0.1);
    dariadb::storage::Manifest::stop();
  }

  if (dariadb::utils::fs::path_exists(storage_path)) {
    dariadb::utils::fs::rm(storage_path);
  }
}

BOOST_AUTO_TEST_CASE(CapManager_Instance) {
	const std::string storagePath = "testStorage";
	const size_t max_size = 10;
	if (dariadb::utils::fs::path_exists(storagePath)) {
		dariadb::utils::fs::rm(storagePath);
	}
    dariadb::storage::Manifest::start(
        dariadb::utils::fs::append_path(storagePath, "Manifest"));

	dariadb::storage::CapacitorManager::start(dariadb::storage::CapacitorManager::Params(storagePath, max_size));

    BOOST_CHECK(dariadb::storage::CapacitorManager::instance() != nullptr);

    auto cap_files = dariadb::utils::fs::ls(storagePath, dariadb::storage::CAP_FILE_EXT);
    BOOST_CHECK_EQUAL(cap_files.size(),size_t(1));

    dariadb::storage::CapacitorManager::stop();
    dariadb::storage::Manifest::stop();

    if (dariadb::utils::fs::path_exists(storagePath)) {
      dariadb::utils::fs::rm(storagePath);
    }
}

BOOST_AUTO_TEST_CASE(CapManager_CommonTest) {
	const std::string storagePath = "testStorage";
    const size_t max_size = 5;
	const dariadb::Time from = 0;
	const dariadb::Time to = from + 1021;
	const dariadb::Time step = 10;

	if (dariadb::utils::fs::path_exists(storagePath)) {
		dariadb::utils::fs::rm(storagePath);
	}
    {
        dariadb::storage::Manifest::start(
                    dariadb::utils::fs::append_path(storagePath, "Manifest"));
        dariadb::storage::CapacitorManager::start(dariadb::storage::CapacitorManager::Params(storagePath, max_size));

        dariadb_test::storage_test_check(dariadb::storage::CapacitorManager::instance(), from, to, step);

        dariadb::storage::CapacitorManager::stop();
        dariadb::storage::Manifest::stop();
    }
    {
		std::shared_ptr<Moc_Storage> stor(new Moc_Storage);
		stor->writed_count = 0;
        dariadb::storage::Manifest::start(
                    dariadb::utils::fs::append_path(storagePath, "Manifest"));
        dariadb::storage::CapacitorManager::start(dariadb::storage::CapacitorManager::Params(storagePath, max_size));

        dariadb::storage::QueryInterval qi(dariadb::IdArray{0}, dariadb::Flag(), from, to);
        dariadb::Meas::MeasList out;
        dariadb::storage::CapacitorManager::instance()->readInterval(qi)->readAll(&out);
        BOOST_CHECK_EQUAL(out.size(),dariadb_test::copies_count);


        auto closed=dariadb::storage::CapacitorManager::instance()->closed_caps();
		BOOST_CHECK(closed.size() != size_t(0));

		for (auto fname : closed) {
            dariadb::storage::CapacitorManager::instance()->drop_cap(fname, stor.get());
		}
		BOOST_CHECK(stor->writed_count != size_t(0));
		
        closed = dariadb::storage::CapacitorManager::instance()->closed_caps();
		BOOST_CHECK_EQUAL(closed.size(),size_t(0));

        dariadb::storage::CapacitorManager::stop();
        dariadb::storage::Manifest::stop();
    }
    if (dariadb::utils::fs::path_exists(storagePath)) {
      dariadb::utils::fs::rm(storagePath);
    }
}
*/
