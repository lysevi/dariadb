#pragma once

#include "exception.h"
#include "meas.h"
#include "compression/binarybuffer.h"
#include "compression/delta.h"
#include "compression/xor.h"
#include "compression/flag.h"
#include <memory>

namespace memseries {
    namespace compression {
        class CopmressedWriter {
        public:
            CopmressedWriter() = default;
            CopmressedWriter(BinaryBuffer bw_time, BinaryBuffer bw_values,
                             BinaryBuffer bw_flags);
            ~CopmressedWriter();
            CopmressedWriter(const CopmressedWriter &other);
            void swap(CopmressedWriter &other);
            CopmressedWriter& operator=(CopmressedWriter &other);
            CopmressedWriter& operator=(CopmressedWriter &&other);


            bool append(const Meas &m);

            bool is_full() const { return _is_full; }

        protected:
            Meas _first;
            bool _is_first;
            bool _is_full;
            DeltaCompressor time_comp;
            XorCompressor value_comp;
            FlagCompressor flag_comp;
        };

        class CopmressedReader {
        public:
            CopmressedReader() = default;
            CopmressedReader(BinaryBuffer bw_time, BinaryBuffer bw_values,
                             BinaryBuffer bw_flags, Meas first);
            ~CopmressedReader();

            Meas read();
            bool is_full() const {
                return this->time_dcomp.is_full() || this->value_dcomp.is_full() ||
                        this->flag_dcomp.is_full();
            }

        protected:
            Meas _first;

            DeltaDeCompressor time_dcomp;
            XorDeCompressor value_dcomp;
            FlagDeCompressor flag_dcomp;
        };
    }
}
