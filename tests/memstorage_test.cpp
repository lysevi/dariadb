#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main

#include <boost/test/unit_test.hpp>
#include <libdariadb/ads/fixed_tree.h>
#include <libdariadb/ads/lockfree_array.h>
#include <libdariadb/storage/memstorage.h>
#include <libdariadb/storage/callbacks.h>
#include <libdariadb/meas.h>
#include <libdariadb/utils/fs.h>

#include <boost/date_time/posix_time/posix_time.hpp>

#include "test_common.h"

class MokChunkWriter :public dariadb::storage::IChunkWriter {
public:
	size_t droped;
	MokChunkWriter() { droped = 0; }
	void appendChunks(const dariadb::storage::ChunksList&chunks) override{
		++droped;
	};
};

BOOST_AUTO_TEST_CASE(LockFreeArrayTypeTraitTest) {
  dariadb::ads::LockFreeArray<int> lf(10);
  BOOST_CHECK_EQUAL(lf.size(), size_t(10));

  dariadb::ads::LockFreeArray<int *> lfs(10);
  BOOST_CHECK_EQUAL(lfs.size(), size_t(10));
}

BOOST_AUTO_TEST_CASE(ArrayLockFreeTest) {
  {
    dariadb::ads::LockFreeArray<bool> lf(3);
    BOOST_CHECK_EQUAL(lf.size(), size_t(3));

    lf.store(0, true);
    lf.store(1, true);

    dariadb::ads::LockFreeArray<bool> lf_c(std::move(lf));
    BOOST_CHECK(lf_c[0]);
    BOOST_CHECK(lf_c[1]);
    BOOST_CHECK(!lf_c[2]);

    bool expected = false;
    BOOST_CHECK(!lf_c.compare_exchange(0, expected, false));
    BOOST_CHECK(expected);
    expected = true;
    BOOST_CHECK(lf_c.compare_exchange(0, expected, false));
    BOOST_CHECK(!lf_c[0]);
  }
  {
    dariadb::ads::LockFreeArray<bool> lf(5);
    BOOST_CHECK_EQUAL(lf.cap(), lf.size());

    size_t i = 0;
    while (lf.insert(true)) {
      i++;
    }
    // check ctors
    dariadb::ads::LockFreeArray<bool> midle(lf);
    dariadb::ads::LockFreeArray<bool> lf_c = midle;

    BOOST_CHECK_EQUAL(lf_c.size(), i);
    BOOST_CHECK_EQUAL(lf_c.cap(), size_t(0));
    for (i = 0; i < lf_c.size(); ++i) {
      BOOST_CHECK(lf_c[i]);
    }
  }
}

template <class T> struct KeySplitter {
  static const size_t levels_count = sizeof(T);
  typedef std::array<size_t, levels_count> splited_key;
  size_t level_size(size_t level_num) const {
    auto res = std::pow(2, sizeof(uint8_t) * 8);
    return res;
  }

  splited_key split(const T &k) const {
    splited_key result;
    auto in_bts = reinterpret_cast<const uint8_t *>(&k);
    for (size_t i = 0; i < levels_count; ++i) {
      result[levels_count - i - 1] = in_bts[i];
    }
    return result;
  }
};

template <typename T> struct Statistic {
  void append(const T &t) {}
};

BOOST_AUTO_TEST_CASE(FixedTreeTypeTraitsTest) {
  dariadb::ads::FixedTree<dariadb::Time, dariadb::Meas, KeySplitter<dariadb::Time>,
                          Statistic<dariadb::Meas>>
      tree;
  BOOST_CHECK_EQUAL(tree.keys_count(), size_t(0));
}

BOOST_AUTO_TEST_CASE(FixedTreeNodeTest) {
  using MeasTree =
      dariadb::ads::FixedTree<dariadb::Time, dariadb::Meas, KeySplitter<dariadb::Time>,
                              Statistic<dariadb::Meas>>;
  MeasTree tree;
  MeasTree::Node node2(&tree, 0, 2);
  BOOST_CHECK(!node2.childExists(0));
  BOOST_CHECK(!node2.childExists(1));

  auto child0 = node2.create_or_get(0);
  auto child1 = node2.create_or_get(1);

  BOOST_CHECK(child0 != nullptr);
  BOOST_CHECK(child1 != nullptr);

  auto child01 = node2.create_or_get(0);
  auto child11 = node2.create_or_get(1);

  BOOST_CHECK_EQUAL(child0, child01);
  BOOST_CHECK_EQUAL(child1, child11);
}

BOOST_AUTO_TEST_CASE(FixedTreeNodeInsertionTest) {
  using TestTree =
      dariadb::ads::FixedTree<uint16_t, int, KeySplitter<uint16_t>, Statistic<int>>;
  TestTree tree;
  uint16_t K1 = 0;
  int V1 = 1;
  tree.insert(K1, V1);
  BOOST_CHECK_EQUAL(tree.keys_count(), size_t(1));
  int result = 0;
  BOOST_CHECK(tree.find(K1, &result));
  BOOST_CHECK_EQUAL(result, V1);

  for (uint16_t i = 1; i < 1000; ++i) {
    auto cur_V = int(i);
    tree.insert(i, int(i));
    BOOST_CHECK(tree.find(i, &result));
    BOOST_CHECK_EQUAL(result, cur_V);
  }

  for (uint16_t i = 2000; i > 1500; --i) {
    auto cur_V = int(i);
    tree.insert(i, int(i));
    BOOST_CHECK(tree.find(i, &result));
    BOOST_CHECK_EQUAL(result, cur_V);
  }

  for (uint16_t i = 1100; i < 1300; ++i) {
    auto cur_V = int(i);
    tree.insert(i, int(i));
    BOOST_CHECK(tree.find(i, &result));
    BOOST_CHECK_EQUAL(result, cur_V);
  }
}

BOOST_AUTO_TEST_CASE(MemChunkAllocatorTest) {
	const size_t buffer_size = 100;
	const size_t max_size = 1024;
	dariadb::storage::MemChunkAllocator allocator(max_size, buffer_size);
	std::set<dariadb::storage::ChunkHeader*> allocated_headers;
	std::set<uint8_t*> allocated_buffers;
	std::set<size_t> positions;

	dariadb::storage::MemChunkAllocator::allocated_data last;
	do {
		auto allocated = allocator.allocate();
		auto hdr = std::get<0>(allocated);
		auto buf = std::get<1>(allocated);
		auto pos = std::get<2>(allocated);
		if (hdr == nullptr) {
			break;
		}
		last = allocated;
		allocated_headers.insert(hdr);
		allocated_buffers.insert(buf);
		positions.insert(pos);
	} while (1);

	BOOST_CHECK(positions.size() > 0);
	BOOST_CHECK_EQUAL(positions.size(),allocated_headers.size());
	BOOST_CHECK_EQUAL(positions.size(), allocated_buffers.size());
	
	allocator.free(last);
	auto new_obj = allocator.allocate();
	BOOST_CHECK_EQUAL(std::get<2>(new_obj), std::get<2>(last));
}

BOOST_AUTO_TEST_CASE(MemStorageCommonTest) {
	auto storage_path = "testMemoryStorage";
	if (dariadb::utils::fs::path_exists(storage_path)) {
		dariadb::utils::fs::rm(storage_path);
	}
	{
		auto settings = dariadb::storage::Settings_ptr{ new dariadb::storage::Settings(storage_path) };
		settings->page_chunk_size = 128;
		dariadb::storage::MemStorage ms{ settings };

		dariadb_test::storage_test_check(&ms, 0, 100, 1, false);
	}
	if (dariadb::utils::fs::path_exists(storage_path)) {
		dariadb::utils::fs::rm(storage_path);
	}
}

BOOST_AUTO_TEST_CASE(MemStorageRndWriteTest) {
	auto storage_path = "testMemoryStorage";
	if (dariadb::utils::fs::path_exists(storage_path)) {
		dariadb::utils::fs::rm(storage_path);
	}
	{
		auto settings = dariadb::storage::Settings_ptr{ new dariadb::storage::Settings(storage_path) };
		settings->page_chunk_size = 128;
		dariadb::storage::MemStorage ms{ settings };

		const dariadb::Time from = 0;
		const dariadb::Time to = 1000;
		const dariadb::Time step = 10;
		const size_t copies_for_id = 100;

		dariadb::Id id_val(0);
		dariadb::Flag flg_val(0);
		dariadb::IdSet _all_ids_set;
		std::vector<dariadb::Time> rnd_times;
		rnd_times.resize(copies_for_id + 2);
		dariadb::Time t = 0;
		auto m = dariadb::Meas::empty();
		size_t total_count = 0;
		for (size_t i = 0; i < rnd_times.size(); ++i) {
			rnd_times[i] = t;
			++t;
		}
		size_t t_pos = 0;
		for (auto i = from; i < to; i += step) {
			std::random_shuffle(rnd_times.begin(), rnd_times.end());
			t_pos = 0;
			_all_ids_set.insert(id_val);
			m.id = id_val;
			m.flag = flg_val;
			m.time = rnd_times[t_pos++];
			m.value = 0;

			for (size_t j = 0; j < copies_for_id; j++) {
				BOOST_CHECK(ms.append(m).writed == 1);
				total_count++;
				m.value = dariadb::Value(j);
				m.time = rnd_times[t_pos++];
			}
			++id_val;
			++flg_val;
		}

		size_t total_readed = 0;
		for (dariadb::Id cur_id = 0; cur_id < id_val; ++cur_id) {
			auto rdr = std::make_shared<dariadb::storage::MList_ReaderClb>();
			dariadb::IdArray ids;
			ids.push_back(cur_id);
			ms.foreach(dariadb::storage::QueryInterval(ids, 0, from, to), rdr.get());
			total_readed += rdr->mlist.size();
			if (rdr->mlist.size() != copies_for_id) {
				BOOST_CHECK(rdr->mlist.size() == copies_for_id);
			}
			for (auto it = rdr->mlist.begin(); it != rdr->mlist.end(); ++it) {
				auto next = it;
				++next;
				if (next == rdr->mlist.end()) {
					break;
				}
				BOOST_CHECK_LE(it->time, next->time);
			}
		}
		BOOST_CHECK_GE(total_count, total_readed);
	}
	if (dariadb::utils::fs::path_exists(storage_path)) {
		dariadb::utils::fs::rm(storage_path);
	}
}


//
//BOOST_AUTO_TEST_CASE(MemStorageDropByLimitTest) {
//	auto storage_path = "testMemoryStorage";
//	if (dariadb::utils::fs::path_exists(storage_path)) {
//		dariadb::utils::fs::rm(storage_path);
//	}
//	{
//		auto settings = dariadb::storage::Settings_ptr{ new dariadb::storage::Settings(storage_path) };
//		settings->page_chunk_size = 128;
//		dariadb::storage::MemStorage ms{ settings };
//		MokChunkWriter*cw = new MokChunkWriter;
//		ms.setDownLevel(cw);
//
//		auto e = dariadb::Meas::empty();
//		while (cw->droped != 10) {
//			e.time++;
//			ms.append(e);
//		}
//		
//	}
//	if (dariadb::utils::fs::path_exists(storage_path)) {
//		dariadb::utils::fs::rm(storage_path);
//	}
//}