#pragma once

#include "meas.h"

namespace memseries{
    namespace statistic{
        class BaseIntegral{
        public:
            BaseIntegral(){_is_first=true;}
            virtual ~BaseIntegral()=default;
            void call(const memseries::Meas&m);
            virtual void calc(const memseries::Meas&a,const memseries::Meas&b)=0;
			virtual memseries::Value result()const = 0;
        protected:
            memseries::Meas _last;
            bool _is_first;
        };
    }
}
