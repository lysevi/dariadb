#include "storage.h"
#include "meas.h"
#include "flags.h"
#include <map>

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
	ByStepClbk(memseries::storage::ReaderClb*clb, memseries::IdArray ids, memseries::Time from, memseries::Time to, memseries::Time step) {
		_out_clbk = clb;
		_step = step;
        _from=from;
        _to=to;

		for (auto id : ids) {
			_last[id].id = id;
			_last[id].time = _from;
			_last[id].flag = memseries::Flags::NO_DATA;
			_last[id].value = 0;

			_isFirst[id] = true;
			
			_new_time_point[id] = from;
		}
	}
	~ByStepClbk() {}
	void call(const Meas&m) {
		if (_isFirst[m.id]) {
			_isFirst[m.id] = false;

			if (m.time > _from) {
				auto cp = _last[m.id];
				for (size_t i = _from; i < m.time; i+=_step) {
					_out_clbk->call(cp);
					cp.time = _last[m.id].time + _step;
					_last[m.id] = cp;
					_new_time_point[m.id] += _step;
				}
				_last_out[m.id] = _last[m.id];
			}
			
			_last[m.id] = m;
		
			_out_clbk->call(_last[m.id]);
			_last_out[m.id] = _last[m.id];
			_new_time_point[m.id] = (m.time + _step);
			return;
		}
		
		if (m.time < _new_time_point[m.id]) {
			_last[m.id] = m;
			return;
		}
		if (m.time == _new_time_point[m.id]) {
			_last[m.id] = m;
			memseries::Meas cp{ m };
			cp.time = _new_time_point[m.id];
			_new_time_point[m.id] = (m.time + _step);
			_last_out[m.id] = cp;
			_out_clbk->call(cp);
			return;
		}
		if (m.time > _new_time_point[m.id]) {
			memseries::Meas cp{ _last[m.id] };
			// get all from _new_time_point to m.time  with step
			for (size_t i = _new_time_point[m.id]; i < m.time; i += _step) {
				cp.time = i;
				_out_clbk->call(cp);
				cp.time = _last[m.id].time + _step;
				_last[m.id] = cp;
				_new_time_point[m.id] += _step;
			}
			if (m.time == _new_time_point[m.id]) {
				_out_clbk->call(m);
				_new_time_point[m.id] = (m.time + _step);
			}
			_last[m.id] = m;
			return;
		}
	}

	memseries::storage::ReaderClb *_out_clbk;
	
	std::map<memseries::Id, bool> _isFirst;
	std::map<memseries::Id, memseries::Meas> _last;
	std::map<memseries::Id, memseries::Meas> _last_out;
	std::map<memseries::Id, memseries::Time> _new_time_point;

	memseries::Time _step;
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
    std::unique_ptr<ByStepClbk> inner_clb(new ByStepClbk(clb,this->getIds(),from,to,step));
	while (!isEnd()) {
		readNext(inner_clb.get());
	}
	for (auto kv : inner_clb->_new_time_point) {
		if (kv.second < to) {
			auto cp = inner_clb->_last[kv.first];
			cp.time = to;
			inner_clb->call(cp);
		}
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
