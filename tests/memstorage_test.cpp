#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Main

#include <boost/test/unit_test.hpp>
#include <libdariadb/storage/memstorage.h>
#include <libdariadb/storage/callbacks.h>
#include <libdariadb/meas.h>
#include <libdariadb/utils/fs.h>
#include <libdariadb/utils/thread_manager.h>

#include <boost/date_time/posix_time/posix_time.hpp>

#include "test_common.h"

class MokChunkWriter:public dariadb::storage::IChunkContainer {
public:
	size_t droped;
	MokChunkWriter() { droped = 0; }
    ~MokChunkWriter(){}
    using IChunkContainer::foreach;

    void appendChunks(const std::vector<dariadb::storage::Chunk*>&a,size_t count) override{
        droped+=count;
    }

	bool minMaxTime(dariadb::Id id, dariadb::Time *minResult, dariadb::Time *maxResult) override {
		return false;
	}
	dariadb::storage::ChunkLinkList chunksByIterval(const dariadb::storage::QueryInterval &query) override {
		return dariadb::storage::ChunkLinkList{};
	}
    dariadb::Id2Meas valuesBeforeTimePoint(const dariadb::storage::QueryTimePoint &q)override {
		return dariadb::Id2Meas{};
	}
	void readLinks(const dariadb::storage::QueryInterval &query, const dariadb::storage::ChunkLinkList &links, dariadb::storage::IReaderClb *clb) override {
	}
};

BOOST_AUTO_TEST_CASE(MemChunkAllocatorTest) {
	const size_t buffer_size = 100;
	const size_t max_size = 1024;
	dariadb::storage::MemChunkAllocator allocator(max_size, buffer_size);
	std::set<dariadb::storage::ChunkHeader*> allocated_headers;
	std::set<uint8_t*> allocated_buffers;
	std::set<size_t> positions;

	dariadb::storage::MemChunkAllocator::AllocatedData last;
	do {
		auto allocated = allocator.allocate();
		auto hdr =  allocated.header;
		auto buf = allocated.buffer;
		auto pos = allocated.position;
		if (hdr == nullptr) {
			break;
		}
		last = allocated;
		allocated_headers.emplace(hdr);
		allocated_buffers.emplace(buf);
		positions.insert(pos);
	} while (1);

	BOOST_CHECK(positions.size() > 0);
	BOOST_CHECK_EQUAL(positions.size(),allocated_headers.size());
	BOOST_CHECK_EQUAL(positions.size(), allocated_buffers.size());
	
	allocator.free(last);
	auto new_obj = allocator.allocate();
	BOOST_CHECK_EQUAL(new_obj.position, last.position);
}

BOOST_AUTO_TEST_CASE(MemStorageCommonTest) {
	auto storage_path = "testMemoryStorage";
	if (dariadb::utils::fs::path_exists(storage_path)) {
		dariadb::utils::fs::rm(storage_path);
	}
	{
		auto settings = dariadb::storage::Settings_ptr{ new dariadb::storage::Settings(storage_path) };
		settings->chunk_size.value = 128;
        dariadb::storage::MemStorage ms{ settings, size_t(0) };

		dariadb_test::storage_test_check(&ms, 0, 100, 1, false);
	}
	if (dariadb::utils::fs::path_exists(storage_path)) {
		dariadb::utils::fs::rm(storage_path);
	}
}

BOOST_AUTO_TEST_CASE(MemStorageDropByLimitTest) {
    auto storage_path = "testMemoryStorage";
    if (dariadb::utils::fs::path_exists(storage_path)) {
        dariadb::utils::fs::rm(storage_path);
    }
	MokChunkWriter*cw = new MokChunkWriter;
    {
        auto settings = dariadb::storage::Settings_ptr{ new dariadb::storage::Settings(storage_path) };
		dariadb::utils::async::ThreadManager::start(settings->thread_pools_params());
        settings->memory_limit.value =1024*1024;
        settings->chunk_size.value = 128;
        dariadb::storage::MemStorage ms{ settings, size_t(0) };
        
        ms.setDownLevel(cw);

        auto e = dariadb::Meas::empty();
        while (true) {
            e.time++;
            ms.append(e);
            if(cw->droped!=0){
                break;
            }
        }
		
    }
	delete cw;
	dariadb::utils::async::ThreadManager::stop();
    if (dariadb::utils::fs::path_exists(storage_path)) {
        dariadb::utils::fs::rm(storage_path);
    }
}
