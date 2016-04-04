#include "cursor.h"
#include "page.h"
#include "bloom_filter.h"

#include <algorithm>
#include <cassert>
using namespace dariadb;
using namespace dariadb::storage;

class Cursor_ListAppend_callback:public Cursor::Callback{
  public:
    ChuncksList*_out;
    Cursor_ListAppend_callback(ChuncksList*out){
        _out=out;

    }
    void call(Chunk_Ptr &ptr) override{
        _out->push_back(ptr);
    }
};

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
    _index_end = link->index + link->header->pos_index;
	_index_it = link->index;
}

Cursor::~Cursor() {
	if (link != nullptr) {
		link->dec_reader();
		link = nullptr;
	}
}

bool Cursor::is_end()const {
	return _is_end;
}

void Cursor::readNext( Cursor::Callback*cbk) {
    std::lock_guard<std::mutex> lg(_mutex);
	for (; !_is_end; _index_it++) {
		if (_index_it == _index_end) {
			_is_end = true;
            break;
		}
        if(!_index_it->is_init){
            continue;
        }

		if ((_ids.size() != 0) && (std::find(_ids.begin(), _ids.end(), _index_it->info.first.id) == _ids.end())) {
			continue;
		}

        if (!dariadb::storage::bloom_check(_index_it->info.flag_bloom, _flag)) {
			continue;
		}

		if ((dariadb::utils::inInterval(_from, _to, _index_it->info.minTime)) || (dariadb::utils::inInterval(_from, _to, _index_it->info.maxTime))) {
            auto ptr=new Chunk(_index_it->info, link->chunks + _index_it->offset, link->header->chunk_size);
            Chunk_Ptr c{ptr};
			assert(c->last.time != 0);
            cbk->call(c);
			_index_it++;
            break;
		}
	}
}

void Cursor::readAll(ChuncksList*output){
    std::unique_ptr<Cursor_ListAppend_callback> clbk{new Cursor_ListAppend_callback{output}};
    readAll(clbk.get());
}

void Cursor::readAll(Callback*cbk) {
	while (!_is_end) {
        readNext(cbk);
	}
}
