#include "page_manager.h"
#include "utils.h"
#include "storage/page.h"

#include <mutex>
#include <cstring>

using namespace dariadb::storage;
dariadb::storage::PageManager* PageManager::_instance = nullptr;

class PageManager::Private
{
public:
    Private(STORAGE_MODE mode,size_t chunk_per_storage, size_t chunk_size) :
		_chunk_per_storage(static_cast<uint32_t>(chunk_per_storage)),
		_chunk_size(static_cast<uint32_t>(chunk_size)),
        _cur_page(nullptr),
        _mode(mode)
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
		return sizeof(PageHeader)
			+ sz_index
			+ sz_buffers;
	}

	Page* create_page() {
		auto sz = calc_page_size();
		auto region = new uint8_t[sz];
		std::fill(region, region + sz, 0);

		auto res = new Page;
		res->region = region;
		res->header = reinterpret_cast<PageHeader*>(region);
		res->index =  reinterpret_cast<Page_ChunkIndex*>(region+sizeof(PageHeader));
		res->chunks = reinterpret_cast<uint8_t*>(region + sizeof(PageHeader) + sizeof(Page_ChunkIndex)*_chunk_per_storage);
		
		res->header->chunk_per_storage = _chunk_per_storage;
		res->header->chunk_size = _chunk_size;
		res->header->maxTime = dariadb::Time(0);
		res->header->minTime = std::numeric_limits<dariadb::Time>::max();
		return res;
		
	}

	Page* get_cur_page() {
		if (_cur_page == nullptr) {
			_cur_page = create_page();
		}
		return _cur_page;
	}

	bool append_chunk(const Chunk_Ptr&ch) {
		std::lock_guard<std::mutex> lg(_mutex);
		auto pg=get_cur_page();
        return pg->append(ch,_mode);
	}

	ChuncksList get_chunks(const dariadb::IdArray&ids, dariadb::Time from, dariadb::Time to, dariadb::Flag flag) {
		//TODO read must be lockfree.
		std::lock_guard<std::mutex> lg(_mutex);
		auto p = get_cur_page();

		return p->get_chunks(ids, from, to, flag);
	}

protected:
	uint32_t _chunk_per_storage;
	uint32_t _chunk_size;
	Page*  _cur_page;
    STORAGE_MODE _mode;
	std::mutex _mutex;
};

PageManager::PageManager(STORAGE_MODE mode, size_t chunk_per_storage, size_t chunk_size):
    impl(new PageManager::Private{mode,chunk_per_storage,chunk_size})
{
	
}

PageManager::~PageManager() {
}

void PageManager::start(STORAGE_MODE mode, size_t chunk_per_storage, size_t chunk_size){
    if(PageManager::_instance==nullptr){
        PageManager::_instance=new PageManager(mode, chunk_per_storage,chunk_size);
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
