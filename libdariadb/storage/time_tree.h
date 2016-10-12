#pragma once

#include <libdariadb/meas.h>
#include <libdariadb/interfaces/imeasstorage.h>
#include <memory>

namespace dariadb {
namespace storage {
	class TimeTree: public IMeasStorage {
	public:
		struct Params{
		};
	public:
		TimeTree(const Params &p);
		~TimeTree();

		// Inherited via IMeasStorage
		virtual Time minTime() override;
		virtual Time maxTime() override;
		virtual bool minMaxTime(dariadb::Id id, dariadb::Time * minResult, dariadb::Time * maxResult) override;
		virtual void foreach(const QueryInterval & q, IReaderClb * clbk) override;
		virtual Id2Meas readTimePoint(const QueryTimePoint & q) override;
		virtual Id2Meas currentValue(const IdArray & ids, const Flag & flag) override;
		virtual append_result append(const Meas & value) override;
		virtual void flush() override;
	private:
		struct Private;
		std::unique_ptr<Private> _impl;
	};
}
}