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
    Private(const std::string &path,
            MODE mode, size_t chunk_per_storage, size_t chunk_size,
            const dariadb::Time write_window_deep, const size_t cap_max_size,
            const dariadb::Time old_mem_chunks):
		mem_storage{ new MemoryStorage(chunk_size) },
		_path(path),
		_mode(mode),
		_chunk_per_storage(chunk_per_storage),
		_chunk_size(chunk_size),
		_write_window_deep(write_window_deep),
        _cap_max_size(cap_max_size),
        _old_mem_chunks(old_mem_chunks)
	{
		mem_cap = new Capacitor(_cap_max_size, mem_storage, _write_window_deep);
        mem_storage_raw=dynamic_cast<MemoryStorage*>(mem_storage.get());
        assert(mem_storage_raw!=nullptr);

        PageManager::start(path,mode,chunk_per_storage,chunk_size);
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
            result=result+this->append(begin[i]);
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

        auto old_chunks=mem_storage_raw->drop_old_chunks(_old_mem_chunks);
        for(auto&c:old_chunks){
            PageManager::instance()->append_chunk(c);
        }
		return result;
	}
	
	void subscribe(const IdArray&ids, const Flag& flag, const ReaderClb_ptr &clbk) {
		mem_storage->subscribe(ids, flag, clbk);
	}
	
	Reader_ptr currentValue(const IdArray&ids, const Flag& flag) {
		return mem_storage->currentValue(ids, flag);
	}
	
	void flush(){
		this->mem_cap->flush();
		auto all_chunks = this->mem_storage_raw->drop_all();
		for (auto c : all_chunks) {
			PageManager::instance()->append_chunk(c);
		}
	}

	ChuncksList chunksByIterval(const IdArray &ids, Flag flag, Time from, Time to) {
		ChuncksList page_chunks,mem_chunks;
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
		auto page_ids=PageManager::instance()->getIds();
		auto mem_ids=mem_storage_raw->getIds();
		dariadb::IdSet s;
		for (auto v : page_ids) {
			s.insert(v);
		}
		for (auto v : mem_ids) {
			s.insert(v);
		}
		return dariadb::IdArray{ s.begin(),s.end() };
	}
    storage::BaseStorage_ptr mem_storage;
    storage::MemoryStorage* mem_storage_raw;
	storage::Capacitor* mem_cap;
	std::string _path;
    MODE _mode;
	size_t _chunk_per_storage;
	size_t _chunk_size;
	dariadb::Time _write_window_deep;
	size_t _cap_max_size;
    dariadb::Time _old_mem_chunks;
};

UnionStorage::UnionStorage(const std::string &path,
                           MODE mode, size_t chunk_per_storage, size_t chunk_size,
                           const dariadb::Time write_window_deep, const size_t cap_max_size,
                           const dariadb::Time old_mem_chunks):
    _impl{new UnionStorage::Private(path,
                                    mode,chunk_per_storage,chunk_size,
                                    write_window_deep, cap_max_size,
                                    old_mem_chunks)}
{
}

UnionStorage::~UnionStorage() {
}

Time UnionStorage::minTime(){
	return _impl->minTime();
}

Time UnionStorage::maxTime(){
	return _impl->maxTime();
}

append_result UnionStorage::append(const Meas::PMeas begin, const size_t size){
	return _impl->append(begin, size);
}

append_result UnionStorage::append(const Meas &value){
	return _impl->append(value);
}

void UnionStorage::subscribe(const IdArray&ids, const Flag& flag, const ReaderClb_ptr &clbk){
	_impl->subscribe(ids, flag, clbk);
}

Reader_ptr UnionStorage::currentValue(const IdArray&ids, const Flag& flag){
	return _impl->currentValue(ids, flag);
}

void  UnionStorage::flush(){
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
