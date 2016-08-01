#include "options.h"

using namespace dariadb::storage;

Options *Options::_instance=nullptr;


void Options::start(){
    if(_instance==nullptr){
        _instance=new Options();
    }
}

void Options::start(const std::string&path) {
	Options::start();
	Options::instance()->path = path;
}

void Options::calc_params() {
	aof_max_size = measurements_count();
}

void Options::set_default() {
	aof_buffer_size = 1000;
	cap_B = CAP_B;
	cap_store_period = 0; // 1000 * 60 * 60;
	cap_max_levels = 11;
	cap_max_closed_caps = 0; // 5;
	page_chunk_size = CHUNK_SIZE;
	
	calc_params();
}

void Options::stop(){
    delete _instance;
    _instance=nullptr;
}

