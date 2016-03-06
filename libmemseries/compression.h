#pragma once

#include "compression/binarybuffer.h"
#include "meas.h"

namespace memseries {
    namespace compression {

        class CopmressedWriter {
        public:
            CopmressedWriter();
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
            class Private;
            Private *_Impl;
        };

        class CopmressedReader {
        public:
            CopmressedReader();
            CopmressedReader(BinaryBuffer bw_time,
                             BinaryBuffer bw_values,
                             BinaryBuffer bw_flags,
                             Meas first);
            ~CopmressedReader();
            CopmressedReader(const CopmressedReader &other);
            void swap(CopmressedReader &other);
            CopmressedReader &operator=(CopmressedReader &other);
            CopmressedReader &operator=(CopmressedReader &&other);

            Meas read();
            bool is_full() const;

        protected:
            class Private;
            Private *_Impl;
        };
    }
}
