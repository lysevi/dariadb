#pragma once

#include "compression/binarybuffer.h"
#include "meas.h"
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
            CopmressedWriter &operator=(CopmressedWriter &other);
            CopmressedWriter &operator=(CopmressedWriter &&other);

            bool append(const Meas &m);
            bool is_full() const;

        protected:
            class Impl;
            Impl *_Impl;
        };

        class CopmressedReader {
        public:
            CopmressedReader() = default;
            CopmressedReader(BinaryBuffer bw_time,
                             BinaryBuffer bw_values,
                             BinaryBuffer bw_flags,
                             Meas first);
            ~CopmressedReader();

            Meas read();
            bool is_full() const;

        protected:
            class Impl;
            Impl *_Impl;
        };
    }
}
