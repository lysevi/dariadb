#include "options.h"
#include "../utils/fs.h"
#include "../utils/exception.h"
#include "../utils/logger.h"
#include <fstream>
#include <extern/json/json.hpp>

using namespace dariadb::storage;
using json = nlohmann::json;

Options *Options::_instance=nullptr;

std::string options_file_in_path(const std::string&path) {
	return dariadb::utils::fs::append_path(path, OPTIONS_FILE_NAME);
}

void Options::start(){
    if(_instance==nullptr){
        _instance=new Options();
    }
}

void Options::start(const std::string&path) {
	Options::start();
	Options::instance()->path = path;
	auto f = options_file_in_path(path);
	if (utils::fs::path_exists(f)) {
		Options::instance()->load(f);
	}
}

void Options::calc_params() {
	aof_max_size = measurements_count();
}

void Options::set_default() {
	logger("options: set default options");
	aof_buffer_size = 1000;
	cap_B = CAP_B;
	cap_store_period = 0; // 1000 * 60 * 60;
	cap_max_levels = 11;
	cap_max_closed_caps = 0; // 5;
	page_chunk_size = CHUNK_SIZE;
	
	calc_params();
}

void Options::stop(){
	if (_instance->path != "") {
		_instance->save();
	}
    delete _instance;
    _instance=nullptr;
}

void Options::save() {
	save(options_file_in_path(_instance->path));
}

void Options::save(const std::string&file) {
	logger("options: save to " << file);
	json js;
	
	js["aof_max_size"] = aof_max_size;
	js["aof_buffer_size"]=aof_buffer_size;

							
	js["cap_B"]=cap_B;
	js["cap_max_levels"]=cap_max_levels;
	js["cap_store_period"]=cap_store_period;
	js["cap_max_closed_caps"]=cap_max_closed_caps;

	js["page_chunk_size"]=page_chunk_size;
	js["page_openned_page_chache_size"]=page_openned_page_chache_size;


	std::fstream fs;
	fs.open(file, std::ios::out);
	if (!fs.is_open()) {
		throw MAKE_EXCEPTION("!fs.is_open()");
	}
	fs << js.dump();
	fs.flush();
	fs.close();
}

void  Options::load(const std::string&file) {
	logger("options: loading " << file);
	std::string content = dariadb::utils::fs::read_file(file);
	json js = json::parse(content);
	aof_max_size=js["aof_max_size"];
	aof_buffer_size=js["aof_buffer_size"] ;


	cap_B = js["cap_B"];
	cap_max_levels = js["cap_max_levels"];
	cap_store_period=js["cap_store_period"] ;
	cap_max_closed_caps=js["cap_max_closed_caps"] ;

	page_chunk_size=js["page_chunk_size"] ;
	page_openned_page_chache_size=js["page_openned_page_chache_size"] ;
}