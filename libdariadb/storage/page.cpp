#include "page.h"
#include "bloom_filter.h"
#include <fstream>
#include <cassert>
#include <algorithm>
#include <cstring>

using namespace dariadb::storage;


class PageCursor : public dariadb::storage::Cursor {
public:
	PageCursor(Page*page, const dariadb::IdArray&ids, dariadb::Time from, dariadb::Time to, dariadb::Flag flag) :
		link(page),
		_ids(ids),
		_from(from),
		_to(to),
		_flag(flag)
	{
		reset_pos();
	}

	~PageCursor() {
		if (link != nullptr) {
			link->dec_reader();
			link = nullptr;
		}
	}

	bool is_end()const override {
		return _is_end;
	}

	void readNext(Cursor::Callback*cbk)  override {
		//TODO refact this
		std::lock_guard<std::mutex> lg(_locker);
        if(read_poses.empty()){
			_is_end = true;
            return;
        }
        auto _index_it=this->link->index[read_poses.front()];
        read_poses.pop_front();
        for (; !_is_end; ) {
            if (_is_end) {
				Chunk_Ptr empty;
				cbk->call(empty);
				_is_end = true;
				break;
			}
		
            if ((_ids.size() != 0) && (std::find(_ids.begin(), _ids.end(), _index_it.info.first.id) == _ids.end())) {
				if (!read_poses.empty()) {
					_index_it = this->link->index[read_poses.front()];
					read_poses.pop_front();
				}
				else { break; }
				continue;
			}

            if (!dariadb::storage::bloom_check(_index_it.info.flag_bloom, _flag)) {
				if (!read_poses.empty()) {
					_index_it = this->link->index[read_poses.front()];
					read_poses.pop_front();
				}
				else { break; }
				continue;
			}

			if (check_index_rec(_index_it)) {
                auto ptr = new Chunk(_index_it.info, link->chunks + _index_it.offset, link->header->chunk_size);
				Chunk_Ptr c{ ptr };
				assert(c->last.time != 0);
				cbk->call(c);
				break;
			}
			else {//end of data;
				_is_end = true;
				Chunk_Ptr empty;
				cbk->call(empty);
				break;
			}
		}
		if (read_poses.empty()) {
			_is_end = true;
			return;
		}
	}
	
    bool check_index_rec(Page_ChunkIndex&it) const{
        return ((dariadb::utils::inInterval(_from, _to, it.info.minTime)) || (dariadb::utils::inInterval(_from, _to, it.info.maxTime)));
	}

	void reset_pos() override { //start read from begining;
		_is_end = false; 
        this->read_poses.clear();
		//TODO lock this->link; does'n need when call from ctor.
		auto sz = this->link->_itree.size();
		if (sz != 0) {
			auto it_to = this->link->_itree.end();// this->link->_itree.upper_bound(this->_to);
			auto it_from = this->link->_itree.begin();//this->link->_itree.lower_bound(this->_from);

            /*if(it_from!= this->link->_itree.begin()){
                if(it_from->first!=this->_from){
                    --it_from;
                }
            }*/
            for(auto it=it_from;it!=this->link->_itree.end();++it){
                this->read_poses.push_back(it->second);
                if(it==it_to){
                    break;
                }
            }
            if(read_poses.empty()){
                _is_end=true;
                return;
            }
        }else{
            _is_end=true;
            return;
        }
	}
protected:
	Page* link;
	bool _is_end;
	dariadb::IdArray _ids;
	dariadb::Time _from, _to;
	dariadb::Flag _flag;
	std::mutex _locker;
    std::list<uint32_t> read_poses;
};


Page::~Page() {
	region=nullptr;
	header=nullptr;
	index=nullptr;
	chunks=nullptr;
	mmap->close();
}

Page* Page::create(std::string file_name, uint64_t sz, uint32_t chunk_per_storage, uint32_t chunk_size, MODE mode) {
	auto res = new Page;
	auto mmap = utils::fs::MappedFile::touch(file_name, sz);
	auto region = mmap->data();
	std::fill(region, region + sz, 0);

	res->mmap = mmap;
	res->region = region;
	res->header = reinterpret_cast<PageHeader*>(region);
	res->index = reinterpret_cast<Page_ChunkIndex*>(region + sizeof(PageHeader));
	res->chunks = reinterpret_cast<uint8_t*>(region + sizeof(PageHeader) + sizeof(Page_ChunkIndex)*chunk_per_storage);

	res->header->chunk_per_storage = chunk_per_storage;
	res->header->chunk_size = chunk_size;
	res->header->maxTime = dariadb::Time(0);
	res->header->minTime = std::numeric_limits<dariadb::Time>::max();
    res->header->is_overwrite=false;
    res->header->mode=mode;

	for (uint32_t i = 0; i < res->header->chunk_per_storage; ++i) {
		assert(!irec->is_init);
		res->_free_poses.push_back(i);
	}
	return res;
}

size_t get_header_offset(){
    return 0;
}

size_t get_index_offset(){
    return sizeof(PageHeader);
}

size_t get_chunks_offset(uint32_t chunk_per_storage){
    return sizeof(PageHeader) + sizeof(Page_ChunkIndex)*chunk_per_storage;
}

Page* Page::open(std::string file_name) {
	auto res = new Page;
	auto mmap = utils::fs::MappedFile::open(file_name);

	auto region = mmap->data();

	res->mmap = mmap;
	res->region = region;
    res->header = reinterpret_cast<PageHeader*>(region)+get_header_offset();
    res->index = reinterpret_cast<Page_ChunkIndex*>(region + get_index_offset());
    res->chunks = reinterpret_cast<uint8_t*>(region + get_chunks_offset(res->header->chunk_per_storage));
	if (res->header->chunk_size == 0) {
		throw MAKE_EXCEPTION("(res->header->chunk_size == 0)");
	}

	for (uint32_t i = 0; i < res->header->chunk_per_storage; ++i) {
		auto irec = &res->index[i];
		if (!irec->is_init) {
			res->_free_poses.push_back(i);
		}
		else {
			res->_itree.insert(std::make_pair(irec->info.maxTime, i));
		}
	}
	return res;
}



PageHeader Page::readHeader(std::string file_name) {
	std::ifstream istream;
	istream.open(file_name, std::fstream::in | std::fstream::binary);
	if (!istream.is_open()) {
		std::stringstream ss;
		ss << "can't open file. filename=" << file_name;
		throw MAKE_EXCEPTION(ss.str());
	}
	PageHeader result;
	memset(&result, 0, sizeof(PageHeader));
	istream.read((char *)&result, sizeof(PageHeader));
	istream.close();
	return result;

}

uint32_t Page::get_oldes_index() {
    auto min_time = std::numeric_limits<dariadb::Time>::max();
    uint32_t pos = 0;
    auto index_end = index + header->chunk_per_storage;
    uint32_t i = 0;
    for (auto index_it = index; index_it != index_end; ++index_it, ++i) {
        if (index_it->info.minTime<min_time) {
            pos = i;
            min_time = index_it->info.minTime;
        }
    }
    return pos;
}

bool Page::append(const ChunksList&ch){
    for(auto &c:ch){
        if(!this->append(c)){
            return false;
        }
    }
    return true;
}
bool Page::append(const Chunk_Ptr&ch) {
    std::lock_guard<std::mutex> lg(_locker);
	auto index_rec = (ChunkIndexInfo*)ch.get();
	auto buffer = ch->_buffer_t.data();

	assert(ch->last.time != 0);
	assert(header->chunk_size == ch->_buffer_t.size());
	uint32_t pos_index=0;
	if (is_full()) {
        if (header->mode == MODE::SINGLE) {
            header->is_overwrite=true;
			pos_index = this->_itree.begin()->second;
        }
		else {
			return false;
		}
	}
	else {
		pos_index = _free_poses.front();
		_free_poses.pop_front();
	}	
	index[pos_index].info = *index_rec;
    index[pos_index].is_init = true;
	

    if(!header->is_overwrite){
        index[pos_index].offset = header->pos_chunks;
        header->pos_chunks += header->chunk_size;
        header->addeded_chunks++;
		_itree.insert(std::make_pair(index_rec->maxTime, pos_index));
	}
	else {
		auto it = this->_itree.begin();
		_itree.erase(it);
		_itree.insert(std::make_pair(index_rec->maxTime, pos_index));
	}
	memcpy(this->chunks + index[pos_index].offset, buffer, sizeof(uint8_t)*header->chunk_size);

	header->minTime = std::min(header->minTime,ch->minTime);
	header->maxTime = std::max(header->maxTime, ch->maxTime);

    this->mmap->flush(get_header_offset(),sizeof(PageHeader));
    this->mmap->flush(get_index_offset()+sizeof(Page_ChunkIndex),sizeof(Page_ChunkIndex));
    auto offset=get_chunks_offset(header->chunk_per_storage)+size_t(this->chunks - index[pos_index].offset);
    this->mmap->flush(offset,sizeof(header->chunk_size));
    return true;
}

bool Page::is_full()const {
	return this->_free_poses.empty();
}

Cursor_ptr Page::get_chunks(const dariadb::IdArray&ids, dariadb::Time from, dariadb::Time to, dariadb::Flag flag) {
    std::lock_guard<std::mutex> lg(_locker);

	auto raw_ptr = new PageCursor(this,ids,from,to,flag );
	Cursor_ptr result{ raw_ptr };

	header->count_readers++;
	
	return result;
}

ChunksList Page::get_open_chunks() {
    std::lock_guard<std::mutex> lg(_locker);
	auto index_end = this->index + this->header->chunk_per_storage;
	auto index_it = this->index;
	ChunksList result;
	for (uint32_t pos = 0; index_it != index_end; ++index_it, ++pos) {
		if (!index_it->is_init) {
			continue;
		}
		if (!index_it->info.is_readonly) {
			index_it->is_init = false;
            auto ptr=new Chunk(index_it->info, this->chunks + index_it->offset, this->header->chunk_size);
            Chunk_Ptr c = Chunk_Ptr(ptr);
			result.push_back(c);
			index_it->is_init = false;
			this->header->addeded_chunks--;
			_free_poses.push_back(pos);
		}
	}
	return result;
}

void Page::dec_reader(){
    std::lock_guard<std::mutex> lg(_locker);
	header->count_readers--;
}

Cursor_ptr dariadb::storage::Page::chunksByIterval(const IdArray & ids, Flag flag, Time from, Time to)
{
	return get_chunks(ids, from, to, flag);
}

IdToChunkMap dariadb::storage::Page::chunksBeforeTimePoint(const IdArray & ids, Flag flag, Time timePoint)
{
	IdToChunkMap result;

	ChunksList ch_list;
	auto cursor = this->get_chunks(ids, header->minTime, timePoint, flag);
	if (cursor == nullptr) {
		return result;
	}
	cursor->readAll(&ch_list);

	for (auto&v : ch_list) {
		auto find_res = result.find(v->first.id);
		if (find_res == result.end()) {
			result.insert(std::make_pair(v->first.id, v));
		}
		else {
			if (find_res->second->maxTime < v->maxTime) {
				result[v->first.id] = v;
			}
		}
	}
	return result;
}
class CountOfIdCallback :public Cursor::Callback {
public:
	dariadb::IdSet ids;
	CountOfIdCallback() {
	}
	~CountOfIdCallback() {
	}

	virtual void call(Chunk_Ptr & ptr) override {
		if (ptr != nullptr) {
			ids.insert(ptr->first.id);
		}
	}
};


dariadb::IdArray dariadb::storage::Page::getIds() 
{
	auto cursor = get_chunks(dariadb::IdArray{}, header->minTime, header->maxTime, 0);

	CountOfIdCallback*clbk_raw = new CountOfIdCallback;
	std::unique_ptr<Cursor::Callback> clbk{ clbk_raw };

	cursor->readAll(clbk.get());

	return dariadb::IdArray{ clbk_raw->ids.begin(),clbk_raw->ids.end() };
}
