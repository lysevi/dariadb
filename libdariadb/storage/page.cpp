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
		std::lock_guard<dariadb::utils::Locker> lg(_locker);
		for (; !_is_end; _index_it++) {
			if (_index_it == _index_end) {
				Chunk_Ptr empty;
				cbk->call(empty);
				_is_end = true;
				break;
			}
			if (!_index_it->is_init) {
				Chunk_Ptr empty;
				cbk->call(empty);
				_is_end = true;
				continue;
			}

			if ((_ids.size() != 0) && (std::find(_ids.begin(), _ids.end(), _index_it->info.first.id) == _ids.end())) {
				continue;
			}

			if (!dariadb::storage::bloom_check(_index_it->info.flag_bloom, _flag)) {
				continue;
			}

			if (check_index_rec(_index_it)) {
				auto ptr = new Chunk(_index_it->info, link->chunks + _index_it->offset, link->header->chunk_size);
				Chunk_Ptr c{ ptr };
				assert(c->last.time != 0);
				cbk->call(c);
				_index_it++;
				break;
			}
			else {//end of data;
				_is_end = true;
				Chunk_Ptr empty;
				cbk->call(empty);
				break;
			}
		}
	}
	
	bool check_index_rec(Page_ChunkIndex*it) const{
		return ((dariadb::utils::inInterval(_from, _to, it->info.minTime)) || (dariadb::utils::inInterval(_from, _to, it->info.maxTime)));
	}

	void reset_pos() override { //start read from begining;
		_is_end = false;
		_index_end = link->index + link->header->chunk_per_storage;
		_index_it = link->index;

		//move to data begining
		for (; !_is_end; _index_it++) {
			if (_index_it == _index_end) {
				_is_end = true;
				break;
			}
			if (check_index_rec(_index_it)) {
				break;
			}
		}
	}
protected:
	Page* link;
	bool _is_end;
	Page_ChunkIndex *_index_it, *_index_end;
	dariadb::IdArray _ids;
	dariadb::Time _from, _to;
	dariadb::Flag _flag;
	dariadb::utils::Locker _locker;
};


Page::~Page() {
	region=nullptr;
	header=nullptr;
	index=nullptr;
	chunks=nullptr;
	mmap->close();
}

Page* Page::create(std::string file_name, uint64_t sz, uint32_t chunk_per_storage, uint32_t chunk_size) {
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
	return res;
}

Page* Page::open(std::string file_name) {
	auto res = new Page;
	auto mmap = utils::fs::MappedFile::open(file_name);

	auto region = mmap->data();

	res->mmap = mmap;
	res->region = region;
	res->header = reinterpret_cast<PageHeader*>(region);
	res->index = reinterpret_cast<Page_ChunkIndex*>(region + sizeof(PageHeader));
	res->chunks = reinterpret_cast<uint8_t*>(region + sizeof(PageHeader) + sizeof(Page_ChunkIndex)*res->header->chunk_per_storage);
	if (res->header->chunk_size == 0) {
		throw MAKE_EXCEPTION("(res->header->chunk_size == 0)");
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

bool Page::append(const Chunk_Ptr&ch, MODE mode) {
    std::lock_guard<dariadb::utils::Locker> lg(_locker);
	auto index_rec = (ChunkIndexInfo*)ch.get();
	auto buffer = ch->_buffer_t.data();

	assert(ch->last.time != 0);
	assert(header->chunk_size == ch->_buffer_t.size());
	if (is_full()) {
        if (mode == MODE::SINGLE) {
            header->is_overwrite=true;
            header->pos_index=0;
        }
		else {
			return false;
		}
	}
	index[header->pos_index].info = *index_rec;

    index[header->pos_index].is_init = true;
	

    if(!header->is_overwrite){
        index[header->pos_index].offset = header->pos_chunks;
        header->pos_chunks += header->chunk_size;
        header->addeded_chunks++;
    }
	memcpy(this->chunks + index[header->pos_index].offset, buffer, sizeof(uint8_t)*header->chunk_size);

	header->pos_index++;
	header->minTime = std::min(header->minTime,ch->minTime);
	header->maxTime = std::max(header->maxTime, ch->maxTime);

	return true;
}

bool Page::is_full()const {
	return !(header->pos_index < header->chunk_per_storage);
}

Cursor_ptr Page::get_chunks(const dariadb::IdArray&ids, dariadb::Time from, dariadb::Time to, dariadb::Flag flag) {
    std::lock_guard<dariadb::utils::Locker> lg(_locker);

	auto raw_ptr = new PageCursor(this,ids,from,to,flag );
	Cursor_ptr result{ raw_ptr };

	header->count_readers++;
	
	return result;
}

ChuncksList Page::get_open_chunks() {
    std::lock_guard<dariadb::utils::Locker> lg(_locker);
	auto index_end = this->index + this->header->pos_index;
	auto index_it = this->index;
	ChuncksList result;
	for (; index_it != index_end; index_it++) {
		if (!index_it->is_init) {
			continue;
		}
		if (!index_it->info.is_readonly) {
			index_it->is_init = false;
            auto ptr=new Chunk(index_it->info, this->chunks + index_it->offset, this->header->chunk_size);
            Chunk_Ptr c = Chunk_Ptr(ptr);
			result.push_back(c);
		}
	}
	return result;
}

void Page::dec_reader(){
    std::lock_guard<dariadb::utils::Locker> lg(_locker);
	header->count_readers--;
}

Cursor_ptr dariadb::storage::Page::chunksByIterval(const IdArray & ids, Flag flag, Time from, Time to)
{
	return get_chunks(ids, from, to, flag);
}

IdToChunkMap dariadb::storage::Page::chunksBeforeTimePoint(const IdArray & ids, Flag flag, Time timePoint)
{
	IdToChunkMap result;

	ChuncksList ch_list;
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