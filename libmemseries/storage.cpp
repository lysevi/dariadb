#include "storage.h"



void memseries::storage::Reader::readAll(Meas::MeasList * output)
{
    while (!isEnd()) {
        readNext(output);
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
