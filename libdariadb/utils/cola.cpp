#include "cola.h"
#include "../compression/cz.h"

using namespace dariadb::utils;


cascading::item::item(){
    is_init=false;
    value=0;
}

cascading::item::item(int v){
    is_init=true;
    value=v;
}

std::string cascading::item::to_string()const{
    std::stringstream ss;
    ss<<value;
    return ss.str();
}

bool cascading::item::operator<(const item&other)const{
    if(is_init && !other.is_init){
        return true;
    }
    return value<other.value;
}

bool cascading::item::operator>(const item&other)const{
    if(is_init && !other.is_init){
        return true;
    }
    return value>other.value;
}

cascading::level::level(size_t _size,size_t lvl_num){
    pos=0;
    size=_size;
    values.resize(size);
    lvl=lvl_num;
}

void cascading::level::clear(){
//            for(size_t i=0;i<values.size();i++){
//                values[i]=item();
//            }
    pos=0;
}

bool cascading::level::free(){
    return (size-pos)!=0;
}

bool cascading::level::is_full(){
    return !free();
}

std::string cascading::level::to_string()const{
    std::stringstream ss;
    ss<<lvl<<": [";
    for(size_t j=0;j<size;++j){
        ss<<values[j].to_string()<<" ";
    }
    ss<<"]";
    return ss.str();
}

void cascading::level::insert(item val){
    if(pos<size){
        values[pos]=val;
        pos++;
    }
}

void cascading::level::merge_with(std::vector<level*> new_values){
    std::vector<size_t> poses(new_values.size());
    std::fill(poses.begin(),poses.end(),size_t(0));
    while(!new_values.empty()){

        //get cur max;
        size_t with_max_index=0;
        item max_val=new_values[0]->values[0];
        for(size_t i=0;i<poses.size();i++){
            if(max_val>new_values[i]->values[poses[i]]){
                  with_max_index=i;
                  max_val=new_values[i]->values[poses[i]];
            }
        }

        this->insert(new_values[with_max_index]->values[poses[with_max_index]]);
        //remove ended in-list
        poses[with_max_index]++;
        if(poses[with_max_index]>=new_values[with_max_index]->size){
            poses.erase(poses.begin()+with_max_index);
            new_values.erase(new_values.begin()+with_max_index);
        }
    }
}

void cascading::alloc_level(size_t num){
    auto nr_ent = size_t(1 << num);
    _levels.push_back(level(nr_ent,num));
    _next_level++;
}

void cascading::resize(size_t levels_count){
    for(size_t i=0;i<levels_count;++i){
        alloc_level(i);
    }
}

void cascading::push(int v){
    size_t new_items_count=_items_count+1;
    size_t outlvl=dariadb::compression::ctz(~_items_count&new_items_count);
    size_t mrg_k=outlvl+1; //k-way merge: k factor
    //std::cout<<"outlvl: "<<uint32_t(outlvl)<<" k:"<<mrg_k <<std::endl;

    if(new_items_count == size_t(1<<_next_level)){
        //std::cout<<"allocate new level: "<<_next_level<<std::endl;
        alloc_level(_next_level);
    }


    std::vector<level*> to_merge(mrg_k);
    level tmp(1,0);
    tmp.insert(v);
    to_merge[0]=&tmp;

    for(size_t i=1;i<=outlvl;++i){
        to_merge[i]=&_levels[i-1];
    }

    auto merge_target=&_levels[outlvl];
    merge_target->merge_with(to_merge);
    for(auto l:to_merge){
        l->clear();
    }
    ++_items_count;
}
