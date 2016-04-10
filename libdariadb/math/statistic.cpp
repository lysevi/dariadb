#include "statistic.h"
#include <cassert>

using namespace  dariadb::statistic;
using namespace  dariadb::statistic::integral;
using namespace  dariadb::statistic::average;

class StatisticReadClbk :public dariadb::storage::ReaderClb {
public:
	StatisticReadClbk(BaseMethod*m):_method(m)
	{
	}
	
	void call(const dariadb::Meas&m) {
		assert(_method != nullptr);
		_method->call(m);
	}
	BaseMethod*_method;
};

BaseMethod::BaseMethod() {
	_is_first = true;
	_result = 0;
}

void BaseMethod::call(const dariadb::Meas&m){
	//TODO add check to m.Id. id must be one.
    if(_is_first){
        _last=m;
        _is_first=false;
    }else{
        this->calc(_last,m);
        _last=m;
    }
}

dariadb::Value BaseMethod::result()const {
	return _result;
}

void BaseMethod::fromReader(dariadb::storage::Reader_ptr&ptr, dariadb::Time from, dariadb::Time to, dariadb::Time step) {
	std::unique_ptr<StatisticReadClbk> c{ new StatisticReadClbk{ this } };
	ptr->readByStep(c.get(), from, to, step);
}

RectangleMethod::RectangleMethod(const RectangleMethod::Kind k): 
	BaseMethod(),
	_kind(k)
{}



void RectangleMethod::calc(const dariadb::Meas&a, const dariadb::Meas&b){
	switch (_kind)
	{
	case Kind::LEFT:
		_result += a.value*(b.time - a.time);
		break;
	case Kind::RIGHT:
		_result += b.value*(b.time - a.time);
		break;
	case Kind::MIDLE:
		_result += ((a.value + b.value) / 2.0)*(b.time - a.time);
		break;
    /*default:
        assert(false);*/
	}
}
Average::Average():BaseMethod() {
	_count = 0;
}

void Average::call(const dariadb::Meas&a){
	_result += a.value;
	_count++;
}

void Average::calc(const dariadb::Meas&, const dariadb::Meas&){
}

dariadb::Value Average::result()const {
	assert(_count != 0);
	return _result / _count;
}
