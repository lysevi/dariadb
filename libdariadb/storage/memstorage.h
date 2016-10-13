#pragma once

#include <libdariadb/meas.h>
#include <libdariadb/interfaces/imeassource.h>
#include <memory>

namespace dariadb {
namespace storage {
	class MemStorage: public IMeasSource {
	public:
		struct Params{
		};
	public:
		MemStorage(const Params &p);
		~MemStorage();

		// Inherited via IMeasStorage
		virtual Time minTime() override;
		virtual Time maxTime() override;
		virtual bool minMaxTime(dariadb::Id id, dariadb::Time * minResult, dariadb::Time * maxResult) override;
		virtual void foreach(const QueryInterval & q, IReaderClb * clbk) override;
		virtual Id2Meas readTimePoint(const QueryTimePoint & q) override;
		virtual Id2Meas currentValue(const IdArray & ids, const Flag & flag) override;
		append_result append(const Time&step, const MeasArray & values);
	private:
		struct Private;
		std::unique_ptr<Private> _impl;
	};
}
}