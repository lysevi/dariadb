#include "page.h"
#include "../bloom_filter.h"
#include <cassert>
#include <algorithm>
#include <mutex>
#include <cstring>

using namespace dariadb::storage;

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
			memcpy(this->chunks + index[pos_index].offset, buffer, sizeof(uint8_t)*header->chunk_size);
			return true;
		}
		return false;
	}
	index[header->pos_index].info = *index_rec;
	index[header->pos_index].offset = header->pos_chunks;
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
	Cursor_ptr result{ new Cursor{this,ids,from,to,flag} };
	if ((from > header->maxTime) || (to < header->minTime) ){
		return result;
	}
	
	header->count_readers++;
	
	return result;
}

Cursor::Cursor(Page*page, const dariadb::IdArray&ids, dariadb::Time from, dariadb::Time to, dariadb::Flag flag):
	link(page),
	_ids(ids),
	_from(from),
	_to(to),
	_flag(flag)
{
	reset_pos();
}

void Cursor::reset_pos() {
	_is_end = false;
	_index_end = link->index + link->header->chunk_per_storage;
	_index_it = link->index;
}

Cursor::~Cursor() {
	if (link != nullptr) {
		//TODO atomic;
		link->header->count_readers--;
		link = nullptr;
	}
}

bool Cursor::is_end()const {
	return _is_end;
}

Chunk_Ptr Cursor::readNext() {
	for (; !_is_end;_index_it++) {
		if (_index_it == _index_end) {
			_is_end = true;
			return nullptr;
		}
		if ((_ids.size() != 0) && (std::find(_ids.begin(), _ids.end(), _index_it->info.first.id) == _ids.end())) {
			continue;
		}

		if (!dariadb::bloom_check(_index_it->info.flag_bloom, _flag)) {
			continue;
		}

		if ((dariadb::utils::inInterval(_from, _to, _index_it->info.minTime)) || (dariadb::utils::inInterval(_from, _to, _index_it->info.maxTime))) {
			Chunk_Ptr c = std::make_shared<Chunk>(_index_it->info, link->chunks + _index_it->offset, link->header->chunk_size);
			_index_it++;
			return c;
		}
	}
	return nullptr;
}

ChuncksList Cursor::readAll() {
	ChuncksList result;
	while (!_is_end) {
		auto c = readNext();
		if (c != nullptr) {
			result.push_back(c);
		}
	}
	return result;
}