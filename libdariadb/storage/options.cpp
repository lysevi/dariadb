#include "options.h"

using namespace dariadb::storage;

Options *Options::_instance=nullptr;


void Options::start(){
    if(_instance==nullptr){
        _instance=new Options();
    }
}
void Options::stop(){
    delete _instance;
    _instance=nullptr;
}

