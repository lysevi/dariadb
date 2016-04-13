#include "page_manager.h"
#include "../utils/utils.h"
#include "page.h"
#include "../utils/fs.h"
#include "../utils/locker.h"

#include <cstring>

using namespace dariadb::storage;
dariadb::storage::PageManager* PageManager::_instance = nullptr;

class PageManager::Private
{
public:
    Private(const PageManager::Params&param) :
        _cur_page(nullptr),
		_param(param)
    {}

    ~Private() {
        if (_cur_page != nullptr) {
            delete _cur_page;
            _cur_page = nullptr;
        }
    }

    uint64_t calc_page_size()const {
        auto sz_index = sizeof(Page_ChunkIndex)*_param.chunk_per_storage;
        auto sz_buffers = _param.chunk_per_storage*_param.chunk_size;
        return sizeof(PageHeader)
                + sz_index
                + sz_buffers;
    }

    Page* create_page() {
        if (!dariadb::utils::fs::path_exists(_param.path)) {
            dariadb::utils::fs::mkdir(_param.path);
        }

        std::string page_name = ((_param.mode == MODE::SINGLE) ? "single.page" : "_.page");
        std::string file_name = dariadb::utils::fs::append_path(_param.path, page_name);

        Page*res = nullptr;

        if (!utils::fs::path_exists(file_name)) {
            auto sz = calc_page_size();
            res = Page::create(file_name, sz,_param.chunk_per_storage,_param.chunk_size);
        }
        else {
            res = Page::open(file_name);
        }
        return res;
    }

    Page* get_cur_page() {
        if (_cur_page == nullptr) {
            _cur_page = create_page();
        }
        return _cur_page;
    }

    bool append_chunk(const Chunk_Ptr&ch) {
        std::lock_guard<dariadb::utils::Locker> lg(_locker);
        auto pg=get_cur_page();
        return pg->append(ch,_param.mode);
    }

    Cursor_ptr chunksByIterval(const IdArray &ids, Flag flag, Time from, Time to){
        std::lock_guard<dariadb::utils::Locker> lg(_locker);
		auto p = get_cur_page();
		return p->chunksByIterval(ids, flag, from, to);
    }

    IdToChunkMap chunksBeforeTimePoint(const IdArray &ids, Flag flag, Time timePoint){
        std::lock_guard<dariadb::utils::Locker> lg(_locker);

		auto cur_page = this->get_cur_page();

		return cur_page->chunksBeforeTimePoint(ids, flag, timePoint);
    }

	
    dariadb::IdArray getIds() {
        std::lock_guard<dariadb::utils::Locker> lg(_locker);
        if(_cur_page==nullptr){
            return dariadb::IdArray{};
        }
		auto cur_page = this->get_cur_page();
		return cur_page->getIds();
    }

	dariadb::storage::ChuncksList get_open_chunks() {
		if(!dariadb::utils::fs::path_exists(_param.path)) {
			return ChuncksList{};
		}
		return this->get_cur_page()->get_open_chunks();
	}

	size_t chunks_in_cur_page() {
		if (_cur_page == nullptr) {
			return 0;
		}
        return _cur_page->header->addeded_chunks;
	}

    dariadb::Time minTime(){
        std::lock_guard<dariadb::utils::Locker> lg(_locker);
        if(_cur_page==nullptr){
            return dariadb::Time(0);
        }else{
            return _cur_page->header->minTime;
        }
    }

    dariadb::Time maxTime(){
        std::lock_guard<dariadb::utils::Locker> lg(_locker);
        if(_cur_page==nullptr){
            return dariadb::Time(0);
        }else{
            return _cur_page->header->maxTime;
        }
    }
protected:
    Page*  _cur_page;
	PageManager::Params _param;
    dariadb::utils::Locker _locker;
};

PageManager::PageManager(const PageManager::Params&param):
    impl(new PageManager::Private{param})
{}

PageManager::~PageManager() {
}

void PageManager::start(const PageManager::Params&param){
    if(PageManager::_instance==nullptr){
        PageManager::_instance=new PageManager(param);
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

//Cursor_ptr PageManager::get_chunks(const dariadb::IdArray&ids, dariadb::Time from, dariadb::Time to, dariadb::Flag flag) {
//    return impl->get_chunks(ids, from, to, flag);
//}

dariadb::storage::Cursor_ptr PageManager::chunksByIterval(const dariadb::IdArray &ids, dariadb::Flag flag, dariadb::Time from, dariadb::Time to){
    return impl->chunksByIterval(ids,flag,from,to);
}

dariadb::storage::IdToChunkMap PageManager::chunksBeforeTimePoint(const dariadb::IdArray &ids, dariadb::Flag flag, dariadb::Time timePoint){
    return impl->chunksBeforeTimePoint(ids,flag,timePoint);
}

dariadb::IdArray PageManager::getIds() {
    return impl->getIds();
}

dariadb::storage::ChuncksList PageManager::get_open_chunks() {
	return impl->get_open_chunks();
}

size_t PageManager::chunks_in_cur_page() const
{
	return impl->chunks_in_cur_page();
}

dariadb::Time PageManager::minTime(){
    return impl->minTime();
}

dariadb::Time PageManager::maxTime(){
    return impl->maxTime();
}
