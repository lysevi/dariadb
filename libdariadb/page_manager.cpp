#include "page_manager.h"

using namespace dariadb::storage;

dariadb::storage::PageManager* PageManager::_instance=nullptr;

struct PageHader {
	uint64_t size;
	uint32_t chunk_per_storage;
	uint32_t chunk_size;
};

PageManager::PageManager(size_t chunk_per_storage, size_t chunk_size):
	_chunk_per_storage(chunk_per_storage),
	_chunk_size(chunk_size)
{}

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


uint64_t PageManager::calc_page_size() {
	return sizeof(PageHader) 
		+ sizeof(ChunkIndexInfo)*_chunk_per_storage 
		+ _chunk_per_storage*_chunk_size;
}

bool PageManager::append_chunk(const Chunk_Ptr&ch) {
	return false;
}