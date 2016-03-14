#pragma once

#include "meas.h"
#include "utils.h"
#include "common.h"
#include <memory>

namespace memseries {
    namespace storage {

        class ReaderClb{
        public:
            virtual void call(const Meas&m)=0;
            virtual ~ReaderClb(){}
        };

        class Reader: public utils::NonCopy {
        public:
            virtual bool isEnd() const = 0;
            virtual void readNext(ReaderClb*clb) = 0;
            virtual void readAll(Meas::MeasList *output);
            virtual void readAll(ReaderClb*clb);
			virtual void readByStep(ReaderClb*clb, memseries::Time step);
			virtual void readByStep(Meas::MeasList *output, memseries::Time step);
        };

        typedef std::shared_ptr<Reader> Reader_ptr;
        class AbstractStorage: public utils::NonCopy {
        public:
            virtual ~AbstractStorage() = default;
            /// min time of writed meas
            virtual Time minTime() = 0;
            /// max time of writed meas
            virtual Time maxTime() = 0;

            virtual append_result append(const Meas::MeasArray &ma);
            virtual append_result append(const Meas::MeasList &ml);
            virtual append_result append(const Meas::PMeas begin, const size_t size) = 0;
            virtual append_result append(const Meas &value) = 0;

            virtual Reader_ptr readInterval(Time from, Time to);
            virtual Reader_ptr readInTimePoint(Time time_point);

            virtual Reader_ptr readInterval(const IdArray &ids,
                                            Flag flag, Time from,
                                            Time to) = 0;
            virtual Reader_ptr readInTimePoint(const IdArray &ids,
                                               Flag flag,
                                               Time time_point) = 0;
        };
		typedef std::shared_ptr<AbstractStorage> AbstractStorage_ptr;
    }
}
