#include "page_manager.h"
#include <mutex>

using namespace dariadb::storage;

dariadb::storage::PageManager* PageManager::_instance=nullptr;

struct PageHader {
	uint64_t size;
	uint32_t chunk_per_storage;
	uint32_t chunk_size;

	uint32_t pos_index;
	uint32_t pos_chunks;

	uint32_t count_readers;
};

struct Page {
	uint8_t        *region;
	PageHader      *header;
	ChunkIndexInfo *index;
	uint8_t        *chunks;
	std::mutex      lock;

	bool append(const Chunk_Ptr&ch) {
		lock.lock();

		if (is_full()) {
			return false;
		}
		auto index_rec = (ChunkIndexInfo*)ch.get();
		auto buffer = ch->_buffer_t.data();

		index[header->pos_index] = *index_rec;
		header->pos_index++;

		memcpy(chunks, buffer, header->chunk_size);
		header->pos_chunks += header->chunk_size;

		lock.unlock();
		return true;
	}

	bool is_full()const {
		return !(header->pos_index < header->chunk_per_storage);
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
		return sizeof(PageHader)
			+ sizeof(ChunkIndexInfo)*_chunk_per_storage
			+ _chunk_per_storage*_chunk_size;
	}

	Page* create_page() {
		auto sz = calc_page_size();
		auto region = new uint8_t[sz];
		std::fill(region, region + sz, 0);

		auto res = new Page;
		res->region = region;
		res->header = reinterpret_cast<PageHader*>(region);
		res->index =  reinterpret_cast<ChunkIndexInfo*>(region+sizeof(PageHader));
		res->chunks = reinterpret_cast<uint8_t*>(region+ sizeof(PageHader)+sizeof(ChunkIndexInfo)*_chunk_per_storage);
		
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