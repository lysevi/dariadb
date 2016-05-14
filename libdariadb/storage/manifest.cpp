#include "manifest.h"
#include "../utils/fs.h"
#include <fstream>
#include <cassert>

dariadb::storage::Manifest::Manifest(const std::string &fname):_filename(fname){
    if(!utils::fs::path_exists(_filename)){
        std::filebuf fbuf;
        fbuf.open(_filename, std::ios_base::in | std::ios_base::out |
                  std::ios_base::trunc);
        fbuf.close();
    }
}

std::list<std::string> dariadb::storage::Manifest::list()
{
    std::list<std::string> result;
    std::ifstream fs;
    fs.open(_filename);
    assert(fs.is_open());
    std::string line;
    while( std::getline (fs,line)){
        result.push_back(line);
    }
    fs.close();
    return result;
}

void dariadb::storage::Manifest::append(const std::string &rec){
    std::ofstream fs;
    fs.open(_filename,std::ios_base::out|std::ios_base::app );
    assert(fs.is_open());
    fs<<rec<<std::endl;
    fs.close();
}
