#pragma once

#include "compression/binarybuffer.h"
#include "meas.h"
#include <memory>

namespace memseries {
    namespace compression {

        class CopmressedWriter {
        public:
            CopmressedWriter();
            CopmressedWriter(const BinaryBuffer &bw_time,
				const BinaryBuffer &bw_values,
				const BinaryBuffer &bw_flags);
            ~CopmressedWriter();
            CopmressedWriter(const CopmressedWriter &other);

            void swap(CopmressedWriter &other);

            CopmressedWriter &operator=(const CopmressedWriter &other);
            CopmressedWriter &operator=(CopmressedWriter &&other);

            bool append(const Meas &m);
            bool is_full() const;

            size_t writed()const;
        protected:
            class Private;
            std::unique_ptr<Private> _Impl;
        };

        class CopmressedReader {
        public:
            CopmressedReader();
            CopmressedReader(const BinaryBuffer &bw_time,
				const BinaryBuffer &bw_values,
				const BinaryBuffer &bw_flags,
				const Meas &first);
            ~CopmressedReader();
            CopmressedReader(const CopmressedReader &other);
            void swap(CopmressedReader &other);
            CopmressedReader &operator=(const CopmressedReader &other);
            CopmressedReader &operator=(CopmressedReader &&other);

            Meas read();
            bool is_full() const;

        protected:
            class Private;
            std::unique_ptr<Private> _Impl;
        };
    }
}
