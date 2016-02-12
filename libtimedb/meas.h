#pragma once

#include <memory>
#include <vector>
#include <list>
#include <ctime>
#include <cstring>
#include <set>
#include <array>
#include "common.h"

namespace timedb {
typedef uint64_t Time;
typedef uint64_t Id;
typedef uint64_t Flag;
typedef uint64_t Value;
typedef std::vector<Id> IdArray;
typedef std::set<Id> IdSet;

struct Meas {
    typedef Meas *PMeas;
    typedef std::vector<Meas> MeasArray;
    typedef std::list<Meas> MeasList;

	Meas();
    void readFrom(const Meas::PMeas m);
    static Meas empty();

    Id id;
    Time time;
    Flag source;
    Flag flag;
    Value value;
    size_t size;
};

}
