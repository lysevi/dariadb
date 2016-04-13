#include "union_storage.h"
#include "storage/memstorage.h"
#include "storage/capacitor.h"
#include "utils/exception.h"
#include "utils/locker.h"
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
		dariadb::storage::ChunkPool::instance()->start(_limits.max_mem_chunks);

		mem_cap = new Capacitor(mem_storage, _cap_params);
		mem_storage_raw = dynamic_cast<MemoryStorage*>(mem_storage.get());
		assert(mem_storage_raw != nullptr);

		PageManager::start(_page_manager_params);

		auto open_chunks = PageManager::instance()->get_open_chunks();
		mem_storage_raw->add_chunks(open_chunks);
	}
	~Private() {
		this->flush();
		delete mem_cap;
		PageManager::stop();
		
	}

	Time minTime() {
        std::lock_guard<dariadb::utils::Locker> lg(_locker);
        if(PageManager::instance()->chunks_in_cur_page()>0){
            return PageManager::instance()->minTime();
        }else{
            return mem_storage->minTime();
        }
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
        std::lock_guard<dariadb::utils::Locker> lg(_locker);
		append_result result{};
        if (!mem_cap->append(value)){
			result.ignored++;
		}
		else {
			result.writed++;
		}

        drop_old_chunks();
		return result;
	}

	void drop_old_chunks() {
		if (_limits.max_mem_chunks == 0) {
			if (_limits.old_mem_chunks != 0) {
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
	}

	void subscribe(const IdArray&ids, const Flag& flag, const ReaderClb_ptr &clbk) {
		mem_storage->subscribe(ids, flag, clbk);
	}

	Reader_ptr currentValue(const IdArray&ids, const Flag& flag) {
		return mem_storage->currentValue(ids, flag);
	}

	void flush() {
        std::lock_guard<dariadb::utils::Locker> lg(_locker);
		this->mem_cap->flush();
		auto all_chunks = this->mem_storage_raw->drop_all();
		for (auto c : all_chunks) {
			PageManager::instance()->append_chunk(c);
		}
	}

	class UnionCursor : public Cursor {
	public:
		Cursor_ptr _page_cursor;
		Cursor_ptr _mem_cursor;
		UnionCursor(Cursor_ptr &page_cursor, Cursor_ptr&mem_cursor):
			_page_cursor{page_cursor},
			_mem_cursor(mem_cursor)
		{
			this->reset_pos();
		}
		~UnionCursor() {
			_page_cursor = nullptr;
			_mem_cursor = nullptr;
		}

		bool is_end()const override {
			return 
				(_page_cursor==nullptr?true:_page_cursor->is_end()) 
				&& (_mem_cursor==nullptr?true:_mem_cursor->is_end());
		}

		void readNext(Cursor::Callback*cbk)  override
		{
			if (!is_end()) {
				if ((_page_cursor!=nullptr) && (!_page_cursor->is_end())) {
					_page_cursor->readNext(cbk);
					return;
				}
				else 
				{
					if ((_mem_cursor!=nullptr)&&(!_mem_cursor->is_end())) {
						_mem_cursor->readNext(cbk);
					}
				}
			}
            Chunk_Ptr empty;
            cbk->call(empty);
		}

		void reset_pos() override {
			if (_page_cursor != nullptr) {
				_page_cursor->reset_pos();
			}
			if (_mem_cursor != nullptr) {
				_mem_cursor->reset_pos();
			}
		}
	};

	Cursor_ptr chunksByIterval(const IdArray &ids, Flag flag, Time from, Time to) {
        std::lock_guard<dariadb::utils::Locker> lg(_locker);
		Cursor_ptr page_chunks, mem_chunks;
		if (from < mem_storage_raw->minTime()) {
			page_chunks = PageManager::instance()->chunksByIterval(ids, flag, from, to);
		}
        if (to > mem_storage_raw->minTime())
        {
			mem_chunks = mem_storage_raw->chunksByIterval(ids, flag, from, to);
		}

		Cursor_ptr result{ new UnionCursor{page_chunks,mem_chunks} };

		return result;
	}

	IdToChunkMap chunksBeforeTimePoint(const IdArray &ids, Flag flag, Time timePoint) {
        std::lock_guard<dariadb::utils::Locker> lg(_locker);
		if (timePoint < mem_storage_raw->minTime()) {
			return PageManager::instance()->chunksBeforeTimePoint(ids, flag, timePoint);
		}
		else {
			return mem_storage_raw->chunksBeforeTimePoint(ids, flag, timePoint);
		}
	}
	IdArray getIds() {
        std::lock_guard<dariadb::utils::Locker> lg(_locker);
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
        std::lock_guard<dariadb::utils::Locker> lg(_locker);
        return mem_storage_raw->chunks_total_size();
    }

	storage::BaseStorage_ptr mem_storage;
	storage::MemoryStorage* mem_storage_raw;
	storage::Capacitor* mem_cap;

	storage::PageManager::Params _page_manager_params;
	dariadb::storage::Capacitor::Params _cap_params;
	dariadb::storage::UnionStorage::Limits _limits;
    mutable dariadb::utils::Locker _locker;
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

Cursor_ptr UnionStorage::chunksByIterval(const IdArray &ids, Flag flag, Time from, Time to) {
	return _impl->chunksByIterval(ids, flag, from, to);
}

IdToChunkMap UnionStorage::chunksBeforeTimePoint(const IdArray &ids, Flag flag, Time timePoint) {
	return _impl->chunksBeforeTimePoint(ids, flag, timePoint);
}

IdArray UnionStorage::getIds() {
	return _impl->getIds();
}

size_t UnionStorage::chunks_in_memory()const{
    return _impl->chunks_in_memory();
}
