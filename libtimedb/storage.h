#pragma once

#include "compression.h"

namespace timedb{
	namespace storage {

		class Reader
		{
		public:
			virtual bool isEnd() const = 0;
			virtual void readNext(Meas::MeasList*output) = 0;
			virtual void readAll(Meas::MeasList*output);
		};

		typedef std::shared_ptr<Reader> Reader_ptr;
		class AbstractStorage {
		public:
			virtual ~AbstractStorage() = default;
			/// min time of writed meas
			virtual Time minTime() = 0;
			/// max time of writed meas
			virtual Time maxTime() = 0;

			virtual append_result append(const Meas& value) = 0;
			virtual append_result append(const Meas::MeasArray& ma);
			virtual append_result append(const Meas::MeasList& ml);
			virtual append_result append(const Meas::PMeas begin, const size_t size) = 0;

			virtual Reader_ptr readInterval(Time from, Time to);
			virtual Reader_ptr readInterval(const IdArray &ids, Flag flag, Time from, Time to) = 0;

			virtual Reader_ptr readInTimePoint(Time time_point);
			virtual Reader_ptr readInTimePoint(const IdArray &ids, Flag flag, Time time_point) = 0;

		};

		class MemoryStorage:public AbstractStorage
		{
			struct Block
			{
                uint8_t *begin;
                uint8_t *end;

				Block(size_t size);
				~Block();
			};

            struct MeasChunk
            {
                Block times;
                Block flags;
                Block values;
                MeasChunk(size_t size);
                ~MeasChunk();
            };
        public:
            class InnerReader:public Reader{
            public:
                bool isEnd() const override;
                void readNext(Meas::MeasList*output)  override;
            };
        public:
			MemoryStorage(size_t size);
			~MemoryStorage();

            using AbstractStorage::append;
            using AbstractStorage::readInterval;
            using AbstractStorage::readInTimePoint;

            Time minTime();
            Time maxTime();
            append_result append(const Meas& value)override;
            append_result append(const Meas::PMeas begin, const size_t size)override;
            Reader_ptr readInterval(const IdArray &ids, Flag flag, Time from, Time to)override;
            Reader_ptr readInTimePoint(const IdArray &ids, Flag flag, Time time_point)override;
		protected:
			size_t _size;
		};
	}
}
