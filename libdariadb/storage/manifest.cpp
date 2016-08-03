#include "manifest.h"
#include "../utils/exception.h"
#include "../utils/fs.h"
#include <algorithm>
#include <cassert>
#include <fstream>
#include <json/json.hpp>

using json = nlohmann::json;
using namespace dariadb::storage;
std::unique_ptr<Manifest> Manifest::_instance;

const std::string PAGE_JS_KEY = "pages";
const std::string COLA_JS_KEY = "cola";
const std::string AOF_JS_KEY = "aof";

Manifest::Manifest(const std::string &fname) : _filename(fname) {
  if (utils::fs::path_exists(_filename)) {
    this->restore();
  }
}

void Manifest::start(const std::string &fname) {
  _instance = std::unique_ptr<Manifest>{new Manifest(fname)};
}

void Manifest::stop() {}

Manifest *Manifest::instance() {
  return Manifest::_instance.get();
}

void Manifest::touch() {
  if (!utils::fs::path_exists(_filename)) {
    json js = json::parse(std::string("{ \"") + COLA_JS_KEY + "\": [], \"" + PAGE_JS_KEY +
                          "\": [] }");

    write_file(_filename, js.dump());
  }
}

void Manifest::restore() {
  std::string storage_path = utils::fs::parent_path(this->_filename);

  auto aofs = this->aof_list();
  aofs.erase(std::remove_if(aofs.begin(), aofs.end(),
                            [this, storage_path](std::string fname) {
                              auto full_file_name =
                                  utils::fs::append_path(storage_path, fname);
                              return !utils::fs::path_exists(full_file_name);
                            }),
             aofs.end());
  clear_field_values(AOF_JS_KEY);
  for (auto fname : aofs) {
    this->aof_append(fname);
  }

  auto caps = this->cola_list();
  caps.erase(std::remove_if(caps.begin(), caps.end(),
                            [this, storage_path](std::string fname) {
                              auto full_file_name =
                                  utils::fs::append_path(storage_path, fname);
                              return !utils::fs::path_exists(full_file_name);
                            }),
             caps.end());
  clear_field_values(COLA_JS_KEY);
  for (auto fname : caps) {
    this->cola_append(fname);
  }

  auto pages = this->page_list();
  pages.erase(std::remove_if(pages.begin(), pages.end(),
                             [this, storage_path](std::string fname) {
                               auto full_file_name =
                                   utils::fs::append_path(storage_path, fname);
                               return !utils::fs::path_exists(full_file_name);
                             }),
              pages.end());
  clear_field_values(PAGE_JS_KEY);
  for (auto fname : pages) {
    this->page_append(fname);
  }
}

std::string Manifest::read_file(const std::string &fname) {
	if(utils::fs::path_exists(fname)){
		return utils::fs::read_file(fname);
	}
	else {
		this->touch();
		return utils::fs::read_file(fname);
	}
}

void Manifest::write_file(const std::string &fname,
                                            const std::string &content) {
  std::fstream fs;
  fs.open(fname, std::ios::out);
  fs << content;
  fs.flush();
  fs.close();
}

std::list<std::string> Manifest::page_list() {
  std::lock_guard<utils::Locker> lg(_locker);

  std::list<std::string> result{};
  json js = json::parse(read_file(_filename));
  for (auto v : js[PAGE_JS_KEY]) {
    result.push_back(v);
  }
  return result;
}

void Manifest::page_append(const std::string &rec) {
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

void Manifest::page_rm(const std::string &rec) {
  std::lock_guard<utils::Locker> lg(_locker);

  json js = json::parse(read_file(_filename));

  std::list<std::string> pg_list{};
  auto pages_json = js[PAGE_JS_KEY];
  for (auto v : pages_json) {
    std::string str_val = v;
    if (rec != str_val) {
      pg_list.push_back(str_val);
    }
  }
  js[PAGE_JS_KEY] = pg_list;
  write_file(_filename, js.dump());
}

std::list<std::string> Manifest::cola_list() {
  std::lock_guard<utils::Locker> lg(_locker);

  std::list<std::string> result{};
  json js = json::parse(read_file(_filename));
  for (auto v : js[COLA_JS_KEY]) {
    result.push_back(v);
  }
  return result;
}

void Manifest::cola_append(const std::string &rec) {
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

void Manifest::cola_rm(const std::string &rec) {
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

std::list<std::string> Manifest::aof_list() {
  std::lock_guard<utils::Locker> lg(_locker);

  std::list<std::string> result{};
  json js = json::parse(read_file(_filename));
  for (auto v : js[AOF_JS_KEY]) {
    result.push_back(v);
  }
  return result;
}

void Manifest::clear_field_values(std::string field_name) {
  json js = json::parse(read_file(_filename));

  std::list<std::string> empty_list{};
  js[field_name] = empty_list;
  write_file(_filename, js.dump());
}

void Manifest::aof_append(const std::string &rec) {
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

void Manifest::aof_rm(const std::string &rec) {
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
