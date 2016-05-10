#pragma once


#include <iostream>

#include <vector>
#include <list>
#include <cstddef>
#include <sstream>
#include <algorithm>
#include <cassert>

namespace dariadb{
    namespace utils{
        class cascading{
            struct item{
                bool is_init;
                int value;
                item();
                item(int v);
                std::string to_string()const;
                bool operator<(const item&other)const;
                bool operator>(const item&other)const;
            };

            struct level{
                std::vector<item> values;
                size_t pos;
                size_t size;
                size_t lvl;

                level()=default;

                level(size_t _size,size_t lvl_num);
                void clear();
                bool free();
                bool is_full();
                std::string to_string()const;
                void insert(item val);
                void merge_with(std::list<level*> new_values);
            };
            void alloc_level(size_t num);
        public:
            cascading():_items_count(0),_next_level(0){
            }

            void resize(size_t levels_count);

            void push(int v);

            void print(){
                std::cout<<"==:\n";
                for(size_t i=0;i<_levels.size();++i){
                    std::cout<<_levels[i].to_string()<<std::endl;
                }
                std::cout<<std::endl;
            }
            size_t levels_count()const{return _levels.size();}
        protected:
            std::vector<level> _levels;
            size_t _items_count;
            size_t _next_level;
        };

    }
}
