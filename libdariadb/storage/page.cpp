#include "page.h"
#include "bloom_filter.h"
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

bool Page::append(const Chunk_Ptr&ch, STORAGE_MODE mode) {
	std::lock_guard<std::mutex> lg(lock);

	auto index_rec = (ChunkIndexInfo*)ch.get();
	auto buffer = ch->_buffer_t.data();

	assert(header->chunk_size == ch->_buffer_t.size());

	if (is_full()) {
		if (mode == STORAGE_MODE::SINGLE) {
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

void Page::dec_reader(){
	std::lock_guard<std::mutex> lg(lock);
	header->count_readers--;
}
