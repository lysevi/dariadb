#include "manifest.h"
#include "../utils/fs.h"
#include "../utils/exception.h"
#include <cassert>
#include <fstream>
#include <json/json.hpp>

using json = nlohmann::json;
using namespace dariadb::storage;
std::unique_ptr<Manifest> Manifest::_instance;

Manifest::Manifest(const std::string &fname)
    : _filename(fname) {
}

void Manifest::start(const std::string & fname){
	_instance = std::unique_ptr<Manifest>{ new Manifest(fname) };
}

void Manifest::stop(){
}

Manifest *Manifest::instance(){
	return Manifest::_instance.get();
}
void dariadb::storage::Manifest::touch(){
    if (!utils::fs::path_exists(_filename)) {
        json js=json::parse("{ \"cola\": [], \"pages\": [] }");

        write_file(_filename,js.dump());
    }
}

std::string dariadb::storage::Manifest::read_file(const std::string&fname){
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
      ss<<line;
    }
    fs.close();
    return ss.str();
}

void dariadb::storage::Manifest::write_file(const std::string &fname,const std::string &content){
    std::fstream fs;
    fs.open(fname, std::ios::out);
    fs << content;
    fs.close();
}

std::list<std::string> dariadb::storage::Manifest::page_list() {
    std::list<std::string> result{};
  json js=json::parse(read_file(_filename));
  for(auto v:js["pages"]){
      result.push_back(v);
  }
  return result;
}

void dariadb::storage::Manifest::page_append(const std::string &rec) {
  json js=json::parse(read_file(_filename));

  std::list<std::string> page_list{};
  auto pages_json=js["pages"];
  for(auto v:pages_json){
      page_list.push_back(v);
  }
  page_list.push_back(rec);
  js["pages"]=page_list;
  write_file(_filename,js.dump());
}
