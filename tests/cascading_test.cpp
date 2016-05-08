#include <iostream>
#include <vector>
#include <list>
#include <cstddef>
#include <sstream>
#include <algorithm>
#include <tuple>
#include <cassert>

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

        bool operator<(const item&other){
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

        void insert(std::vector<item> new_values){
            assert(new_values.size()==size);
            for(;pos<size;pos++){
                values[pos]=new_values[pos];
            }
        }

        void insert(std::vector<item> &a,std::vector<item> &b){
            assert(a.size()==b.size());
            assert((a.size()+b.size())==size);
            size_t aPos,bPos;
            aPos=bPos=0;

            for(pos=0;pos<size;++pos){
                if(a[aPos]<b[bPos]){
                    values[pos]=a[aPos];
                    aPos++;
                }else{
                    values[pos]=b[bPos];
                    bPos++;
                }
            }
        }
    };
public:
    cascading():_memory(B,0){

    }

    void push(int v){
        if(_memory.pos<B){
            _memory.insert(item(v));
        }else{
            std::sort(_memory.values.begin(),_memory.values.end());
            if(_levels.empty()){
                _levels.push_back(level(_memory.size,1));
            }
            if(_levels[0].free()){
                _levels[0].insert(_memory.values);

                _memory.clear();
            }else{
                _levels.push_back(level(B*_levels.back().size,_levels.size()+1));
                for(size_t i=_levels.size()-1;;--i){
                    auto prev=&_levels[i-1];
                    auto pprev= (i==1)? &_memory: &_levels[i-1];
                    _levels[i].insert(pprev->values,prev->values);
                    if(i==1){
                        break;
                    }
                }
                _levels[0].insert(_memory.values);
                _memory.clear();
            }
            _memory.insert(item(v));
        }
    }

    void print(){
        std::cout<<"mem:\n "<<_memory.to_string()<<std::endl;
        std::cout<<"ext:\n";
        for(size_t i=0;i<_levels.size();++i){
            std::cout<<_levels[i].to_string()<<"\n";
        }
        std::cout<<"\n";
    }
protected:
    level _memory;
    std::vector<level> _levels;
};

int main(){
    cascading c;
    c.push(3);
    c.push(1);
    c.print();
    c.push(2);
    c.push(4);
    c.print();
    c.push(7);
    c.push(11);
    c.print();
    c.push(5);
    c.print();
}
