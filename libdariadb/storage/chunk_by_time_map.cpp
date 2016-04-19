#include "chunk_by_time_map.h"

using namespace dariadb::storage;

void ChunkByTimeMap::remove_droped(){
    bool dropped=true;
    while (dropped) {
        dropped = false;
        for (auto i = begin(); i != end(); ) {
            if ((*i).second->is_dropped) {
                erase(i++);
                dropped = true;
                break;
            }
            else {
                ++i;
            }
        }
    }
}

stxMap::const_iterator ChunkByTimeMap::get_upper_bound(Time to)const{
    auto rest = upper_bound(to);

    if ((rest != end()) && (rest->first != to)) {
        ++rest;
    }
    return rest;
}

stxMap::iterator ChunkByTimeMap::get_upper_bound(Time to){
    auto rest = upper_bound(to);

    if ((rest != end()) && (rest->first != to)) {
        ++rest;
    }
    return rest;
}

stxMap::const_iterator ChunkByTimeMap::get_lower_bound(Time from)const{
    auto resf = lower_bound(from);
    if ((resf != begin()) && (resf->first != from)) {
        --resf;
    }
    else {
        if (resf == end()) {
            resf = begin();
        }
    }
    return resf;
}

stxMap::iterator ChunkByTimeMap::get_lower_bound(Time from){
    auto resf = lower_bound(from);
    if ((resf != begin()) && (resf->first != from)) {
        --resf;
    }
    else {
        if (resf == end()) {
            resf = begin();
        }
    }
    return resf;
}
