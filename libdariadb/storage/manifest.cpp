#include "manifest.h"
#include "../utils/exception.h"
#include "../utils/fs.h"
#include <cassert>
#include <fstream>
#include <json/json.hpp>

using json = nlohmann::json;
using namespace dariadb::storage;
std::unique_ptr<Manifest> Manifest::_instance;

const std::string PAGE_JS_KEY = "pages";
const std::string COLA_JS_KEY = "cola";
const std::string AOF_JS_KEY = "aof";

Manifest::Manifest(const std::string &fname) : _filename(fname) {}

void Manifest::start(const std::string &fname) {
  _instance = std::unique_ptr<Manifest>{new Manifest(fname)};
}

void Manifest::stop() {}

Manifest *Manifest::instance() {
  return Manifest::_instance.get();
}
void dariadb::storage::Manifest::touch() {
  if (!utils::fs::path_exists(_filename)) {
    json js = json::parse(std::string("{ \"") + COLA_JS_KEY + "\": [], \"" + PAGE_JS_KEY +
                          "\": [] }");

    write_file(_filename, js.dump());
  }
}

std::string dariadb::storage::Manifest::read_file(const std::string &fname) {
  std::ifstream fs;
  fs.open(fname);
  if (!fs.is_open()) {
    this->touch();
    fs.open(fname);
    if (!fs.is_open()) {
      throw MAKE_EXCEPTION("(!fs.is_open())");
    }
  }

  std::stringstream ss;
  std::string line;
  while (std::getline(fs, line)) {
    ss << line;
  }
  fs.close();
  return ss.str();
}

void dariadb::storage::Manifest::write_file(const std::string &fname,
                                            const std::string &content) {
  std::fstream fs;
  fs.open(fname, std::ios::out);
  fs << content;
  fs.close();
}

std::list<std::string> dariadb::storage::Manifest::page_list() {
  std::lock_guard<utils::Locker> lg(_locker);

  std::list<std::string> result{};
  json js = json::parse(read_file(_filename));
  for (auto v : js[PAGE_JS_KEY]) {
    result.push_back(v);
  }
  return result;
}

void dariadb::storage::Manifest::page_append(const std::string &rec) {
  std::lock_guard<utils::Locker> lg(_locker);

  json js = json::parse(read_file(_filename));

  std::list<std::string> page_list{};
  auto pages_json = js[PAGE_JS_KEY];
  for (auto v : pages_json) {
    page_list.push_back(v);
  }
  page_list.push_back(rec);
  js[PAGE_JS_KEY] = page_list;
  write_file(_filename, js.dump());
}

std::list<std::string> dariadb::storage::Manifest::cola_list() {
  std::lock_guard<utils::Locker> lg(_locker);

  std::list<std::string> result{};
  json js = json::parse(read_file(_filename));
  for (auto v : js[COLA_JS_KEY]) {
    result.push_back(v);
  }
  return result;
}

void dariadb::storage::Manifest::cola_append(const std::string &rec) {
  std::lock_guard<utils::Locker> lg(_locker);

  json js = json::parse(read_file(_filename));

  std::list<std::string> cola_list{};
  auto pages_json = js[COLA_JS_KEY];
  for (auto v : pages_json) {
    cola_list.push_back(v);
  }
  cola_list.push_back(rec);
  js[COLA_JS_KEY] = cola_list;
  write_file(_filename, js.dump());
}

void Manifest::cola_rm(const std::string & rec){
	std::lock_guard<utils::Locker> lg(_locker);

	json js = json::parse(read_file(_filename));

	std::list<std::string> cola_list{};
	auto pages_json = js[COLA_JS_KEY];
	for (auto v : pages_json) {
		std::string str_val = v;
		if (rec != str_val) {
			cola_list.push_back(str_val);
		}
	}
	js[COLA_JS_KEY] = cola_list;
	write_file(_filename, js.dump());
}


std::list<std::string> dariadb::storage::Manifest::aof_list() {
	std::lock_guard<utils::Locker> lg(_locker);

	std::list<std::string> result{};
	json js = json::parse(read_file(_filename));
	for (auto v : js[AOF_JS_KEY]) {
		result.push_back(v);
	}
	return result;
}

void dariadb::storage::Manifest::aof_append(const std::string &rec) {
	std::lock_guard<utils::Locker> lg(_locker);

	json js = json::parse(read_file(_filename));

	std::list<std::string> aof_list{};
	auto pages_json = js[AOF_JS_KEY];
	for (auto v : pages_json) {
		aof_list.push_back(v);
	}
	aof_list.push_back(rec);
	js[AOF_JS_KEY] = aof_list;
	write_file(_filename, js.dump());
}

void Manifest::aof_rm(const std::string & rec) {
	std::lock_guard<utils::Locker> lg(_locker);

	json js = json::parse(read_file(_filename));

	std::list<std::string> aof_list{};
	auto aof_json = js[AOF_JS_KEY];
	for (auto v : aof_json) {
		std::string str_val = v;
		if (rec != str_val) {
			aof_list.push_back(str_val);
		}
	}
	js[AOF_JS_KEY] = aof_list;
	write_file(_filename, js.dump());
}
