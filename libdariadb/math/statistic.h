#pragma once

#include "../meas.h"
#include "../storage.h"

namespace dariadb{
    namespace statistic{
		class BaseMethod {
		public:
			BaseMethod();
			virtual ~BaseMethod() = default;
			virtual void call(const dariadb::Meas&m);
			virtual void calc(const dariadb::Meas&a, const dariadb::Meas&b) = 0;
			virtual dariadb::Value result()const;

			void fromReader(dariadb::storage::Reader_ptr&ptr, dariadb::Time from, dariadb::Time to, dariadb::Time step);
		protected:
			dariadb::Meas _last;
			bool _is_first;
			dariadb::Value _result;
		};

		namespace integral {
			class RectangleMethod : public BaseMethod {
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
		namespace average {
			class Average : public BaseMethod {
			public:
				Average();
				void call(const dariadb::Meas&a) override;
				void calc(const dariadb::Meas&a, const dariadb::Meas&b) override;
				dariadb::Value result()const override;
			protected:
				size_t _count;
			};
		}
    }
}
