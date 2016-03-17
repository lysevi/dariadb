#include "integrall.h"

using namespace  memseries::statistic;

void BaseIntegrall::call(const memseries::Meas&m){
    if(_is_first){
        _last=m;
        _is_first=false;
    }else{
        this->calc(_last,m);
        _last=m;
    }
}
