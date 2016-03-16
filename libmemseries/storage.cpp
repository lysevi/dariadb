#include "storage.h"
#include "meas.h"
#include "flags.h"

using namespace memseries;
using namespace memseries::storage;

class InnerCallback:public memseries::storage::ReaderClb{
public:
    InnerCallback(Meas::MeasList * output){
        _output=output;
    }
    ~InnerCallback(){}
    void call(const Meas&m){
        _output->push_back(m);
    }

    Meas::MeasList *_output;
};

class ByStepClbk :public memseries::storage::ReaderClb {
public:
    ByStepClbk(memseries::storage::ReaderClb*clb,memseries::Time from, memseries::Time to, memseries::Time step) {
		_out_clbk = clb;
		_isFirst = true;
		_step = step;
        _from=from;
        _to=to;
		_last.time = _from;
		_last.flag = memseries::Flags::NO_DATA;
	}
	~ByStepClbk() {}
	void call(const Meas&m) {
		if (_isFirst) {
			_isFirst = false;
			_out_clbk->call(m);
			_last_data = m;
			_last = m;
			_new_time_point = (_last.time + _step);
			return;
		}
		if (m.time < _new_time_point) {
			_last = m;
			return;
		}
		if (m.time == _new_time_point) {
			_last = m;
			memseries::Meas cp{ m };
			cp.time = _new_time_point;
			_new_time_point = (_last.time + _step);
			_last_data = cp;
			_out_clbk->call(cp);
			return;
		}
		if (m.time > _new_time_point) {
			auto delta_time = m.time - _new_time_point;
			memseries::Meas cp{ _last };
			// get all from _new_time_point to m.time  with step
			for (size_t i = 0; i < (delta_time / _step)+1; i++) {
				cp.time = _last_data.time + _step;
				_out_clbk->call(cp);
				_last_data = cp;
				_new_time_point += _step;
			}
			if (m.time == _new_time_point) {
				_out_clbk->call(m);
				_new_time_point = (m.time + _step);
			}
			_last = m;
			return;
		}
	}

	memseries::storage::ReaderClb *_out_clbk;
	bool _isFirst;
	memseries::Meas _last;
	memseries::Meas _last_data;
	memseries::Time _step;
	memseries::Time _new_time_point;
    memseries::Time _from;
    memseries::Time _to;
};

void Reader::readAll(Meas::MeasList * output)
{
    std::unique_ptr<InnerCallback> clb(new InnerCallback(output));
	this->readAll(clb.get());
}


void Reader::readAll(ReaderClb*clb)
{
    while (!isEnd()) {
        readNext(clb);
    }
}

void  Reader::readByStep(ReaderClb*clb, memseries::Time from, memseries::Time to, memseries::Time step) {
    std::unique_ptr<ByStepClbk> inner_clb(new ByStepClbk(clb,from,to,step));
	while (!isEnd()) {
		readNext(inner_clb.get());
	}
	if (inner_clb->_new_time_point > inner_clb->_last.time) {
		inner_clb->_last.time = inner_clb->_new_time_point;
		inner_clb->call(inner_clb->_last);
	}
}

void  Reader::readByStep(Meas::MeasList *output, memseries::Time from, memseries::Time to, memseries::Time step) {
	std::unique_ptr<InnerCallback> clb(new InnerCallback(output));
	this->readByStep(clb.get(),from,to,step);
}

append_result AbstractStorage::append(const Meas::MeasArray & ma)
{
    memseries::append_result ar{};
    for(auto&m:ma){
        ar=ar+this->append(m);
    }
    return ar;
}

append_result AbstractStorage::append(const Meas::MeasList & ml)
{
    memseries::append_result ar{};
    for(auto&m:ml){
        ar=ar+this->append(m);
    }
    return ar;
}

Reader_ptr AbstractStorage::readInterval(Time from, Time to)
{
    static memseries::IdArray empty_id{};
    return this->readInterval(empty_id,0,from,to);
}

Reader_ptr AbstractStorage::readInTimePoint(Time time_point)
{
    static memseries::IdArray empty_id{};
    return this->readInTimePoint(empty_id,0,time_point);
}
