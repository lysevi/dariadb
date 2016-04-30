#pragma once

#include "chunk.h"
#include <stx/btree_multimap>
#include <map>
namespace dariadb{
    namespace storage{

		//using stxMap = std::multimap<dariadb::Time, dariadb::storage::Chunk_Ptr>;
        template<typename Type, typename basseType=stx::btree_multimap<dariadb::Time, Type>>
        class ChunkByTimeMap:public basseType{
        public:
            typedef typename  basseType::const_iterator const_iterator;
            typedef typename  basseType::iterator iterator;
            void remove_droped(){
                bool dropped = true;
                while (dropped) {
                    dropped = false;
                    for (auto i = this->begin(); i != this->end(); ++i) {
                        if ((*i).second->is_dropped) {
                            this->erase(i);
                            dropped = true;
                            break;
                        }
                    }
                }
            }

            const_iterator get_upper_bound(Time to)const{
                auto rest = this->upper_bound(to);

                if ((rest != this->end()) && (rest->first != to)) {
                    ++rest;
                }
                return rest;
            }

            iterator get_upper_bound(Time to){
                auto rest = this->upper_bound(to);

                if ((rest != this->end()) && (rest->first != to)) {
                    ++rest;
                }
                return rest;
            }

            const_iterator get_lower_bound(Time from)const{
                auto resf = this->lower_bound(from);
                if ((resf != this->begin()) && (resf->first != from)) {
                    --resf;
                }
                else {
                    if (resf == this->end()) {
                        resf = this->begin();
                    }
                }
                return resf;
            }

            iterator get_lower_bound(Time from){
                auto resf = this->lower_bound(from);
                if ((resf != this->begin()) && (resf->first != from)) {
                    --resf;
                }
                else {
                    if (resf == this->end()) {
                        resf = this->begin();
                    }
                }
                return resf;
            }
        };
    }
}
