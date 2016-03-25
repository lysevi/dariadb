#include "cursor.h"
#include "page.h"
#include "../bloom_filter.h"

#include <algorithm>

using namespace dariadb;
using namespace dariadb::storage;

Cursor::Cursor(Page*page, const dariadb::IdArray&ids, dariadb::Time from, dariadb::Time to, dariadb::Flag flag) :
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
	for (; !_is_end; _index_it++) {
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