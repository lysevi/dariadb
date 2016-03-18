#pragma once

#include "meas.h"

namespace memseries{
    namespace statistic{
        class BaseIntegral{
        public:
			BaseIntegral();
            virtual ~BaseIntegral()=default;
            void call(const memseries::Meas&m);
            virtual void calc(const memseries::Meas&a,const memseries::Meas&b)=0;
			virtual memseries::Value result()const;
        protected:
            memseries::Meas _last;
            bool _is_first;
			memseries::Value _result;
        };

		class RectangleMethod : public BaseIntegral {
		public:
			enum Kind {
				LEFT,
				RIGHT,
				MIDLE
			};
			RectangleMethod(const Kind k);
			void calc(const memseries::Meas&a, const memseries::Meas&b) override;
		protected:
			Kind _kind;
		};
    }
}
