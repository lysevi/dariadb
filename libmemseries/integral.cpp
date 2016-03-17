#include "integral.h"

using namespace  memseries::statistic;

void BaseIntegral::call(const memseries::Meas&m){
    if(_is_first){
        _last=m;
        _is_first=false;
    }else{
        this->calc(_last,m);
        _last=m;
    }
}
