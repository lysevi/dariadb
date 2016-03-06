#include "storage.h"
#include "meas.h"

using namespace memseries;

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

void memseries::storage::Reader::readAll(Meas::MeasList * output)
{
    std::shared_ptr<InnerCallback> clb(new InnerCallback(output));
    while (!isEnd()) {
        readNext(clb.get());
    }
}


void memseries::storage::Reader::readAll(ReaderClb*clb)
{
    while (!isEnd()) {
        readNext(clb);
    }
}


memseries::append_result memseries::storage::AbstractStorage::append(const Meas::MeasArray & ma)
{
    memseries::append_result ar{};
    for(auto&m:ma){
        this->append(m);
    }
    return ar;
}

memseries::append_result memseries::storage::AbstractStorage::append(const Meas::MeasList & ml)
{
    memseries::append_result ar{};
    for(auto&m:ml){
        ar=ar+this->append(m);
    }
    return ar;
}

memseries::storage::Reader_ptr memseries::storage::AbstractStorage::readInterval(Time from, Time to)
{
    static memseries::IdArray empty_id{};
    return this->readInterval(empty_id,0,from,to);
}

memseries::storage::Reader_ptr memseries::storage::AbstractStorage::readInTimePoint(Time time_point)
{
    static memseries::IdArray empty_id{};
    return this->readInTimePoint(empty_id,0,time_point);
}
