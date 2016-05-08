#include <iostream>
#include <vector>
#include <list>
#include <cstddef>
#include <sstream>
#include <algorithm>
#include <tuple>
#include <cassert>

#include <compression/cz.h>

const size_t B=2;

class cascading{

    struct item{
        bool is_init;
        int value;
        item(){
            is_init=false;
            value=0;
        }
        item(int v){
            is_init=true;
            value=v;
        }

        std::string to_string()const{
            std::stringstream ss;
            ss<<value;
            return ss.str();
        }

        bool operator<(const item&other)const{
            if(is_init && !other.is_init){
                return true;
            }
            return value<other.value;
        }
    };

    struct level{
        std::vector<item> values;
        size_t pos;
        size_t size;
        size_t lvl;

        level()=default;

        level(size_t _size,size_t lvl_num){
            pos=0;
            size=_size;
            values.resize(size);
            lvl=lvl_num;
        }

        void clear(){
            for(size_t i=0;i<values.size();i++){
                values[i]=item();
            }
            pos=0;
        }

        bool free(){
            return (size-pos)!=0;
        }

        bool is_full(){
            return !free();
        }

        std::string to_string()const{
            std::stringstream ss;
            ss<<lvl<<": [";
            for(size_t j=0;j<size;++j){
                ss<<values[j].to_string()<<" ";
            }
            ss<<"]";
            return ss.str();
        }

        void insert(item val){
            if(pos<size){
                values[pos]=val;
                pos++;
            }
        }

        void merge_with(std::vector<level*> new_values){

        }
    };
public:
    cascading():_items_count(0),_next_level(0){

    }

    void push(int v){
        size_t new_items_count=_items_count+1;
        size_t outlvl=dariadb::compression::ctz(~_items_count&new_items_count);
        size_t mrg_k=outlvl+1; //k-way merge: k factor
        std::cout<<"outlvl: "<<uint32_t(outlvl)<<" k:"<<mrg_k <<std::endl;

        if(new_items_count == size_t(1<<_next_level)){
            std::cout<<"allocate new level: "<<_next_level<<std::endl;
            auto nr_ent = size_t(1 << _next_level);
            _levels.push_back(level(nr_ent,_next_level));
            _next_level++;
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
        //auto target=&_levels[outlvl];
//        while(mrg_k){

//            std::cout<<"> "<<_levels[mrg_k-1].to_string()<<std::endl;
//            mrg_k--;
//        }
        ++_items_count;
    }

    void print(){
        std::cout<<"==:\n";
        for(size_t i=0;i<_levels.size();++i){
            std::cout<<_levels[i].to_string()<<std::endl;
        }
        std::cout<<std::endl;
    }
protected:
    std::vector<level> _levels;
    size_t _items_count;
    size_t _next_level;
};

int main(){
    cascading c;
    c.push(3);
    c.push(1);
    c.print();
    c.push(2);
    c.push(4);
    c.print();
//    c.push(7);
//    c.push(11);
//    c.print();
//    c.push(5);
//    c.push(15);
//    c.print();
}
