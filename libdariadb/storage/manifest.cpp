#include "manifest.h"
#include "../utils/fs.h"
#include <fstream>
#include <cassert>

dariadb::storage::Manifest::Manifest(const std::string &fname):_filename(fname){
    if(!utils::fs::path_exists(_filename)){
        std::fstream fs;
        fs.open(_filename, std::ios::out);
        fs.close();
    }
}

std::list<std::string> dariadb::storage::Manifest::page_list()
{
    std::list<std::string> result;
    std::ifstream fs;
    fs.open(_filename);
    if(!fs.is_open()){
        return result;
    }
    std::string line;
    while( std::getline (fs,line)){
        result.push_back(line);
    }
    fs.close();
    return result;
}

void dariadb::storage::Manifest::page_append(const std::string &rec){
    std::ofstream fs;
    fs.open(_filename,std::ios_base::out|std::ios_base::app );
    assert(fs.is_open());
    fs<<rec<<std::endl;
    fs.close();
}
