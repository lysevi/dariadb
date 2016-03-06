#pragma once

#include "binarybuffer.h"
#include "../meas.h"

namespace memseries {
    namespace compression {

        class FlagCompressor {
        public:
            FlagCompressor() = default;
            FlagCompressor(const BinaryBuffer &bw);
            ~FlagCompressor();
            FlagCompressor(const FlagCompressor &other);
            void swap(FlagCompressor &other);
            FlagCompressor& operator=(FlagCompressor &other);
            FlagCompressor& operator=(FlagCompressor &&other);

            bool append(Flag v);

            bool is_full() const { return _bw.is_full(); }

        protected:
            BinaryBuffer _bw;
            bool _is_first;
            Flag _first;
        };

        class FlagDeCompressor {
        public:
            FlagDeCompressor() = default;
            FlagDeCompressor(const BinaryBuffer &bw, Flag first);
            ~FlagDeCompressor() = default;
            FlagDeCompressor(const FlagDeCompressor &other);
            void swap(FlagDeCompressor &other);
            FlagDeCompressor& operator=(FlagDeCompressor &other);

            Flag read();

            bool is_full() const { return _bw.is_full(); }

        protected:
            BinaryBuffer _bw;
            Flag _prev_value;
        };

    }
}
