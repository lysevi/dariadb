#include "cursor.h"

#include <algorithm>
#include <cassert>
using namespace dariadb;
using namespace dariadb::storage;

class Cursor_ListAppend_callback:public Cursor::Callback{
  public:
    dariadb::storage::ChuncksList*_out;
    Cursor_ListAppend_callback(dariadb::storage::ChuncksList*out){
        _out=out;

    }
    void call(Chunk_Ptr &ptr) override{
		if (ptr != nullptr) {
			_out->push_back(ptr);
		}
    }
};

Cursor::~Cursor(){
}

void Cursor::readAll(ChuncksList*output){
    std::unique_ptr<Cursor_ListAppend_callback> clbk{new Cursor_ListAppend_callback{output}};
    readAll(clbk.get());
}

void Cursor::readAll(Callback*cbk) {
	while (!this->is_end()) {
        readNext(cbk);
	}
}
