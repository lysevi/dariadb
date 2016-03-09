#include "storage.h"
#include "meas.h"

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

void Reader::readAll(Meas::MeasList * output)
{
    std::shared_ptr<InnerCallback> clb(new InnerCallback(output));
    while (!isEnd()) {
        readNext(clb.get());
    }
}


void Reader::readAll(ReaderClb*clb)
{
    while (!isEnd()) {
        readNext(clb);
    }
}


append_result AbstractStorage::append(const Meas::MeasArray & ma)
{
    memseries::append_result ar{};
    for(auto&m:ma){
        this->append(m);
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
