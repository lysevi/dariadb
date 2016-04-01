#include "union_storage.h"
#include "storage/memstorage.h"
#include "storage/capacitor.h"
#include "utils/exception.h"
#include "storage/page_manager.h"
#include <cassert>

using namespace dariadb;
using namespace dariadb::storage;

class UnionStorage::Private {
public:
	Private(const PageManager::Params&page_storage_params,
		dariadb::storage::Capacitor::Params cap_params,
		dariadb::storage::UnionStorage::Limits limits) :
		mem_storage{ new MemoryStorage(page_storage_params.chunk_size) },
		_page_manager_params(page_storage_params),
		_cap_params(cap_params),
       _limits(limits)
	{
		mem_cap = new Capacitor(mem_storage, _cap_params);
		mem_storage_raw = dynamic_cast<MemoryStorage*>(mem_storage.get());
		assert(mem_storage_raw != nullptr);

		PageManager::start(_page_manager_params);

		auto open_chunks = PageManager::instance()->get_open_chunks();
		mem_storage_raw->add_chunks(open_chunks);
	}
	~Private() {
		this->flush();
		PageManager::stop();
		delete mem_cap;
	}

	Time minTime() {
		return mem_storage->minTime();
	}

	Time maxTime() {
		return mem_storage->maxTime();
	}

	append_result append(const Meas::PMeas begin, const size_t size) {
		append_result result{};
		for (size_t i = 0; i < size; i++) {
			result = result + this->append(begin[i]);
		}
		return result;
	}

	append_result append(const Meas &value) {
		append_result result{};
		if (!mem_cap->append(value)) {
			result.ignored++;
		}
		else {
			result.writed++;
		}

		if (_limits.max_mem_chunks == 0) {
            if(_limits.old_mem_chunks!=0){
                auto old_chunks = mem_storage_raw->drop_old_chunks(_limits.old_mem_chunks);
                for (auto&c : old_chunks) {
                    PageManager::instance()->append_chunk(c);
                }
            }
		}
		else {
			auto old_chunks = mem_storage_raw->drop_old_chunks_by_limit(_limits.max_mem_chunks);
			for (auto&c : old_chunks) {
				PageManager::instance()->append_chunk(c);
			}
		}
		return result;
	}

	void subscribe(const IdArray&ids, const Flag& flag, const ReaderClb_ptr &clbk) {
		mem_storage->subscribe(ids, flag, clbk);
	}

	Reader_ptr currentValue(const IdArray&ids, const Flag& flag) {
		return mem_storage->currentValue(ids, flag);
	}

	void flush() {
		this->mem_cap->flush();
		auto all_chunks = this->mem_storage_raw->drop_all();
		for (auto c : all_chunks) {
			PageManager::instance()->append_chunk(c);
		}
	}

	ChuncksList chunksByIterval(const IdArray &ids, Flag flag, Time from, Time to) {
		ChuncksList page_chunks, mem_chunks;
		if (from < mem_storage_raw->minTime()) {
			page_chunks = PageManager::instance()->chunksByIterval(ids, flag, from, to);
		}
		if (to > mem_storage_raw->minTime()) {
			mem_chunks = mem_storage_raw->chunksByIterval(ids, flag, from, to);
		}

		for (auto&c : mem_chunks) {
			page_chunks.push_back(c);
		}

		return page_chunks;
	}
	IdToChunkMap chunksBeforeTimePoint(const IdArray &ids, Flag flag, Time timePoint) {
		if (timePoint < mem_storage_raw->minTime()) {
			return PageManager::instance()->chunksBeforeTimePoint(ids, flag, timePoint);
		}
		else {
			return mem_storage_raw->chunksBeforeTimePoint(ids, flag, timePoint);
		}
	}
	IdArray getIds()const {
		auto page_ids = PageManager::instance()->getIds();
		auto mem_ids = mem_storage_raw->getIds();
		dariadb::IdSet s;
		for (auto v : page_ids) {
			s.insert(v);
		}
		for (auto v : mem_ids) {
			s.insert(v);
		}
		return dariadb::IdArray{ s.begin(),s.end() };
	}

    size_t chunks_in_memory()const{
        return mem_storage_raw->chunks_total_size();
    }

	storage::BaseStorage_ptr mem_storage;
	storage::MemoryStorage* mem_storage_raw;
	storage::Capacitor* mem_cap;

	storage::PageManager::Params _page_manager_params;
	dariadb::storage::Capacitor::Params _cap_params;
	dariadb::storage::UnionStorage::Limits _limits;
};

UnionStorage::UnionStorage(storage::PageManager::Params page_manager_params,
	dariadb::storage::Capacitor::Params cap_params,
	const dariadb::storage::UnionStorage::Limits&limits) :
	_impl{ new UnionStorage::Private(page_manager_params,
									cap_params, limits) }
{
}

UnionStorage::~UnionStorage() {
    _impl=nullptr;
}

Time UnionStorage::minTime() {
	return _impl->minTime();
}

Time UnionStorage::maxTime() {
	return _impl->maxTime();
}

append_result UnionStorage::append(const Meas::PMeas begin, const size_t size) {
	return _impl->append(begin, size);
}

append_result UnionStorage::append(const Meas &value) {
	return _impl->append(value);
}

void UnionStorage::subscribe(const IdArray&ids, const Flag& flag, const ReaderClb_ptr &clbk) {
	_impl->subscribe(ids, flag, clbk);
}

Reader_ptr UnionStorage::currentValue(const IdArray&ids, const Flag& flag) {
	return _impl->currentValue(ids, flag);
}

void  UnionStorage::flush() {
	_impl->flush();
}

ChuncksList UnionStorage::chunksByIterval(const IdArray &ids, Flag flag, Time from, Time to) {
	return _impl->chunksByIterval(ids, flag, from, to);
}

IdToChunkMap UnionStorage::chunksBeforeTimePoint(const IdArray &ids, Flag flag, Time timePoint) {
	return _impl->chunksBeforeTimePoint(ids, flag, timePoint);
}

IdArray UnionStorage::getIds()const {
	return _impl->getIds();
}

size_t UnionStorage::chunks_in_memory()const{
    return _impl->chunks_in_memory();
}
