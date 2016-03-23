#include "page_manager.h"

using namespace dariadb::storage;

dariadb::storage::PageManager* PageManager::_instance=nullptr;

PageManager::PageManager(){

}

void PageManager::start(){
    if(PageManager::_instance==nullptr){
        PageManager::_instance=new PageManager();
    }
}

void PageManager::stop(){
    if(_instance!=nullptr){
        delete PageManager::_instance;
    }
}

PageManager* PageManager::instance(){
    return _instance;
}
