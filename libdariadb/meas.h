#pragma once

#include <memory>
#include <vector>
#include <list>
#include <set>
#include <stdint.h>

namespace dariadb {
    typedef uint64_t Time;
    typedef uint64_t Id;
    typedef uint32_t Flag;
    typedef double Value;
    typedef std::vector<Id> IdArray;
    typedef std::set<Id> IdSet;

    bool areSame(Value a, Value b, const Value EPSILON=1E-5);

    struct Meas {
        typedef Meas *PMeas;
        typedef std::vector<Meas> MeasArray;
        typedef std::list<Meas> MeasList;

        static Meas empty();

        Meas();
        void readFrom(const Meas::PMeas m);
        bool operator==(const Meas &other) const {
            return id == other.id && time == other.time && flag == other.flag &&
                    areSame(value, other.value);
        }

        Id id;
        Time time;
        Value value;
        Flag flag;
        Flag src;
    };

    bool in_filter(Flag filter, Flag flg);
}
