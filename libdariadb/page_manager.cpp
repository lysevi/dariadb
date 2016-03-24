#include "page_manager.h"
#include "utils.h"
#include "bloom_filter.h"

#include <mutex>
#include <cstring>
#include <cassert>

using namespace dariadb::storage;

dariadb::storage::PageManager* PageManager::_instance=nullptr;
#pragma pack(push, 1)
struct PageHader {
	uint32_t chunk_per_storage;
	uint32_t chunk_size;

	uint32_t pos_index;
	uint32_t pos_chunks;

	uint32_t count_readers;
};

struct Page_ChunkIndex {
	ChunkIndexInfo info;
	uint64_t       offset;
};
#pragma pack(pop)

struct Page {
	uint8_t        *region;
	PageHader      *header;
	Page_ChunkIndex*index;
	uint8_t        *chunks;
	std::mutex      lock;

	bool append(const Chunk_Ptr&ch) {
		std::lock_guard<std::mutex> lg(lock);

		if (is_full()) {
			return false;
		}

		auto index_rec = (ChunkIndexInfo*)ch.get();
		auto buffer = ch->_buffer_t.data();
		
		assert(header->chunk_size == ch->_buffer_t.size());

		index[header->pos_index].info = *index_rec;
		index[header->pos_index].offset = header->pos_chunks;
		memcpy(this->chunks+header->pos_chunks, buffer, sizeof(uint8_t)*header->chunk_size);
		
		header->pos_chunks += header->chunk_size;
		header->pos_index++;
		return true;
	}

	bool is_full()const {
		return !(header->pos_index < header->chunk_per_storage);
	}

	ChuncksList get_chunks(const dariadb::IdArray&ids, dariadb::Time from, dariadb::Time to, dariadb::Flag flag) {
		header->count_readers++;

		ChuncksList result{};
		auto index_end = index + header->chunk_per_storage;
		for (auto index_it = index; index_it != index_end; ++index_it) {
			if ((ids.size() != 0) && (std::find(ids.begin(), ids.end(), index_it->info.first.id) == ids.end())) {
				continue;
			}

			if (!dariadb::bloom_check(index_it->info.flag_bloom, flag)) {
				continue;
			}

			if ((dariadb::utils::inInterval(from, to, index_it->info.minTime)) || (dariadb::utils::inInterval(from, to, index_it->info.maxTime))) {
				Chunk_Ptr c=std::make_shared<Chunk>(index_it->info, chunks + index_it->offset,header->chunk_size);
				result.push_back(c);
			}
		}
		header->count_readers--;
		return result;
	}
};

class PageManager::Private
{
public:
	Private(size_t chunk_per_storage, size_t chunk_size) :
		_chunk_per_storage(static_cast<uint32_t>(chunk_per_storage)),
		_chunk_size(static_cast<uint32_t>(chunk_size)),
		_cur_page(nullptr)
	{}

	~Private() {
		if (_cur_page != nullptr) {
			delete _cur_page->region;
			delete _cur_page;
			_cur_page = nullptr;
		}
	}

	uint64_t calc_page_size()const {
		auto sz_index = sizeof(Page_ChunkIndex)*_chunk_per_storage;
		auto sz_buffers = _chunk_per_storage*_chunk_size;
		return sizeof(PageHader)
			+ sz_index
			+ sz_buffers;
	}

	Page* create_page() {
		auto sz = calc_page_size();
		auto region = new uint8_t[sz];
		std::fill(region, region + sz, 0);

		auto res = new Page;
		res->region = region;
		res->header = reinterpret_cast<PageHader*>(region);
		res->index =  reinterpret_cast<Page_ChunkIndex*>(region+sizeof(PageHader));
		res->chunks = reinterpret_cast<uint8_t*>(region + sizeof(PageHader) + sizeof(Page_ChunkIndex)*_chunk_per_storage);
		
		res->header->chunk_per_storage = _chunk_per_storage;
		res->header->chunk_size = _chunk_size;
		return res;
		
	}

	Page* get_cur_page() {
		if (_cur_page == nullptr) {
			_cur_page = create_page();
		}
		return _cur_page;
	}

	bool append_chunk(const Chunk_Ptr&ch) {
		auto pg=get_cur_page();
		return pg->append(ch);
	}

	ChuncksList get_chunks(const dariadb::IdArray&ids, dariadb::Time from, dariadb::Time to, dariadb::Flag flag) {
		auto p = get_cur_page();

		return p->get_chunks(ids, from, to, flag);
	}

protected:
	uint32_t _chunk_per_storage;
	uint32_t _chunk_size;
	Page*  _cur_page;
};

PageManager::PageManager(size_t chunk_per_storage, size_t chunk_size):
	impl(new PageManager::Private{chunk_per_storage,chunk_size})
{
	
}

PageManager::~PageManager() {
}

void PageManager::start(size_t chunk_per_storage, size_t chunk_size){
    if(PageManager::_instance==nullptr){
        PageManager::_instance=new PageManager(chunk_per_storage,chunk_size);
    }
}

void PageManager::stop(){
    if(_instance!=nullptr){
        delete PageManager::_instance;
		_instance = nullptr;
    }
}

PageManager* PageManager::instance(){
    return _instance;
}

bool PageManager::append_chunk(const Chunk_Ptr&ch) {
	return impl->append_chunk(ch);
}

ChuncksList PageManager::get_chunks(const dariadb::IdArray&ids, dariadb::Time from, dariadb::Time to, dariadb::Flag flag) {
	return impl->get_chunks(ids, from, to, flag);
}