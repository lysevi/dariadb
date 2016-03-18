#pragma once

#include "meas.h"

namespace dariadb{
    namespace statistic{
		namespace integral {
			class BaseIntegral {
			public:
				BaseIntegral();
				virtual ~BaseIntegral() = default;
				void call(const dariadb::Meas&m);
				virtual void calc(const dariadb::Meas&a, const dariadb::Meas&b) = 0;
				virtual dariadb::Value result()const;
			protected:
				dariadb::Meas _last;
				bool _is_first;
				dariadb::Value _result;
			};

			class RectangleMethod : public BaseIntegral {
			public:
				enum Kind {
					LEFT,
					RIGHT,
					MIDLE
				};
				RectangleMethod(const Kind k);
				void calc(const dariadb::Meas&a, const dariadb::Meas&b) override;
			protected:
				Kind _kind;
			};
		}
    }
}
