#pragma once

#include "meas.h"

namespace memseries{
    namespace statistic{
        class BaseIntegrall{
        public:
            BaseIntegrall(){_is_first=true;}
            virtual ~BaseIntegrall()=default;
            void call(const memseries::Meas&m);
            virtual void calc(const memseries::Meas&a,const memseries::Meas&b)=0;
        protected:
            memseries::Meas _last;
            bool _is_first;
        };
    }
}
