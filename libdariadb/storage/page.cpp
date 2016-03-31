#include "page.h"
#include "bloom_filter.h"
#include <fstream>
#include <cassert>
#include <algorithm>
#include <mutex>
#include <cstring>

using namespace dariadb::storage;

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
	std::lock_guard<std::mutex> lg(lock);

	auto index_rec = (ChunkIndexInfo*)ch.get();
	auto buffer = ch->_buffer_t.data();

	assert(ch->last.time != 0);
	assert(header->chunk_size == ch->_buffer_t.size());

	if (is_full()) {
        if (mode == MODE::SINGLE) {
			auto pos_index = get_oldes_index();

			index[pos_index].info = *index_rec;
            index[pos_index].is_init = true;
			memcpy(this->chunks + index[pos_index].offset, buffer, sizeof(uint8_t)*header->chunk_size);
			return true;
		}
		return false;
	}
	index[header->pos_index].info = *index_rec;
	index[header->pos_index].offset = header->pos_chunks;
    index[header->pos_index].is_init = true;
	memcpy(this->chunks + header->pos_chunks, buffer, sizeof(uint8_t)*header->chunk_size);

	header->pos_chunks += header->chunk_size;
	header->pos_index++;
	header->minTime = std::min(header->minTime,ch->minTime);
	header->maxTime = std::max(header->maxTime, ch->maxTime);
	return true;
}

bool Page::is_full()const {
	return !(header->pos_index < header->chunk_per_storage);
}

Cursor_ptr Page::get_chunks(const dariadb::IdArray&ids, dariadb::Time from, dariadb::Time to, dariadb::Flag flag) {
	if ((from > header->maxTime) || (to < header->minTime) ){
		return nullptr;
	}
	std::lock_guard<std::mutex> lg(lock);

	Cursor_ptr result{ new Cursor{ this,ids,from,to,flag } };

	header->count_readers++;
	
	return result;
}

ChuncksList Page::get_open_chunks() {
	std::lock_guard<std::mutex> lg(lock);
	auto index_end = this->index + this->header->pos_index;
	auto index_it = this->index;
	ChuncksList result;
	for (; index_it != index_end; index_it++) {
		if (!index_it->is_init) {
			continue;
		}
		if (!index_it->info.is_readonly) {
			index_it->is_init = false;
			Chunk_Ptr c = std::make_shared<Chunk>(index_it->info, this->chunks + index_it->offset, this->header->chunk_size);
			result.push_back(c);
		}
	}
	return result;
}

void Page::dec_reader(){
	std::lock_guard<std::mutex> lg(lock);
	header->count_readers--;
}
